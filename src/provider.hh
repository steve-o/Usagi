/* RFA provider.
 */

#ifndef __PROVIDER_HH__
#define __PROVIDER_HH__
#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

/* Boost Atomics */
#include <boost/atomic.hpp>

/* Boost Posix Time */
#include <boost/date_time/posix_time/posix_time.hpp>

/* Boost noncopyable base class */
#include <boost/utility.hpp>

/* Boost threading. */
#include <boost/thread.hpp>

/* RFA 7.2 */
#include <rfa/rfa.hh>

/* ZeroMQ messaging middleware. */
#include <zmq.h>

#include "rfa.hh"
#include "config.hh"
#include "deleter.hh"
#include "provider.pb.h"

namespace usagi
{
/* Performance Counters */
	enum {
		PROVIDER_PC_MSGS_SENT,
		PROVIDER_PC_RFA_MSGS_SENT,
		PROVIDER_PC_RFA_EVENTS_RECEIVED,
		PROVIDER_PC_RFA_EVENTS_DISCARDED,
		PROVIDER_PC_OMM_CMD_ERRORS,
		PROVIDER_PC_CONNECTION_EVENTS_RECEIVED,
		PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED,
		PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_EXCEPTION,
		PROVIDER_PC_CLIENT_SESSION_REJECTED,
		PROVIDER_PC_CLIENT_SESSION_ACCEPTED,
/* marker */
		PROVIDER_PC_MAX
	};

	class client_t;
	class item_stream_t;

	class request_t : boost::noncopyable
	{
	public:
		request_t (std::shared_ptr<item_stream_t>& item_stream_, std::shared_ptr<client_t>& client_, bool use_attribinfo_in_updates_)
			: item_stream (item_stream_),
			  client (client_),
			  use_attribinfo_in_updates (use_attribinfo_in_updates_),
			  has_initial_image (false)
		{
		}

		std::weak_ptr<item_stream_t> item_stream;
		std::weak_ptr<client_t> client;
		const bool use_attribinfo_in_updates;	/* can theoretically change in reissue */
/* RFA will return a CmdError message if the provider application submits data
 * before receiving a login success message.  Mute downstream publishing until
 * permission is granted to submit data.
 */
		boost::atomic_bool has_initial_image;
	};

	class item_stream_t : boost::noncopyable
	{
	public:
		item_stream_t ()
		{
		}

/* Fixed name for this stream. */
		rfa::common::RFA_String rfa_name;
/* Request tokens for clients, can be more than one per client. */
		std::unordered_map<rfa::sessionLayer::RequestToken*const, std::shared_ptr<request_t>> requests;
		boost::shared_mutex lock;
	};

	class provider_t :
		public rfa::common::Client,
		boost::noncopyable
	{
	public:
		provider_t (const config_t& config, std::shared_ptr<rfa_t> rfa, std::shared_ptr<rfa::common::EventQueue> event_queue, std::shared_ptr<void> zmq_context);
		~provider_t();

		bool Init() throw (rfa::common::InvalidConfigurationException, rfa::common::InvalidUsageException);

		bool CreateItemStream (const char* name, std::shared_ptr<item_stream_t> item_stream) throw (rfa::common::InvalidUsageException);
		bool Send (item_stream_t& item_stream, rfa::message::RespMsg& msg, const rfa::message::AttribInfo& attribInfo) throw (rfa::common::InvalidUsageException);
		bool SendReply (rfa::message::RespMsg& msg, rfa::sessionLayer::RequestToken& token) throw (rfa::common::InvalidUsageException);
		uint32_t Submit (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);

/* RFA event callback. */
		void processEvent (const rfa::common::Event& event) override;

		std::shared_ptr<request_t> GetRequest (rfa::sessionLayer::RequestToken*const token) {
			auto it = requests_.find (token);
			return it->second.lock();
		}
		uint16_t GetRwfVersion() const {
			return min_rwf_version_.load();
		}
		const char* GetServiceName() const {
			return config_.service_name.c_str();
		}
		uint32_t GetServiceId() const {
			return service_id_;
		}

	private:
		void OnConnectionEvent (const rfa::sessionLayer::ConnectionEvent& event);
		void OnOMMActiveClientSessionEvent (const rfa::sessionLayer::OMMActiveClientSessionEvent& event);
		void OnOMMCmdErrorEvent (const rfa::sessionLayer::OMMCmdErrorEvent& event);

		bool RejectClientSession (const rfa::common::Handle* handle);
		bool AcceptClientSession (const rfa::common::Handle* handle);
		bool EraseClientSession (rfa::common::Handle* handle);

		void GetDirectoryResponse (rfa::message::RespMsg* msg, uint8_t rwf_major_version, uint8_t rwf_minor_version, const char* service_name, uint32_t filter_mask, uint8_t response_type);
		void GetServiceDirectory (rfa::data::Map* map, rfa::data::SingleWriteIterator* it, uint8_t rwf_major_version, uint8_t rwf_minor_version, const char* service_name, uint32_t filter_mask);
		void GetServiceFilterList (rfa::data::SingleWriteIterator* it, uint8_t rwf_major_version, uint8_t rwf_minor_version, uint32_t filter_mask);
		void GetServiceInformation (rfa::data::SingleWriteIterator* it, uint8_t rwf_major_version, uint8_t rwf_minor_version);
		void GetServiceCapabilities (rfa::data::SingleWriteIterator* it);
		void GetServiceDictionaries (rfa::data::SingleWriteIterator* it);
		void GetServiceState (rfa::data::SingleWriteIterator* it, uint8_t rwf_major_version, uint8_t rwf_minor_version);

		void SetServiceId (uint32_t service_id) {
			service_id_.store (service_id);
		}

		const config_t& config_;

/* RFA context. */
		std::shared_ptr<rfa_t> rfa_;

/* RFA asynchronous event queue. */
		std::shared_ptr<rfa::common::EventQueue> event_queue_;

/* RFA session defines one or more connections for horizontal scaling. */
		std::unique_ptr<rfa::sessionLayer::Session, internal::release_deleter> session_;

/* RFA OMM provider interface. */
		std::unique_ptr<rfa::sessionLayer::OMMProvider, internal::destroy_deleter> omm_provider_;

/* RFA Connection event consumer */
		rfa::common::Handle* connection_item_handle_;
/* RFA Listen event consumer */
		rfa::common::Handle* listen_item_handle_;
/* RFA Error Item event consumer */
		rfa::common::Handle* error_item_handle_;

/* RFA Client Session directory */
		std::unordered_map<rfa::common::Handle*const, std::shared_ptr<client_t>> clients_;
		boost::shared_mutex clients_lock_;

		friend client_t;

/* Entire request set */
		std::unordered_map<rfa::sessionLayer::RequestToken*const, std::weak_ptr<request_t>> requests_;
		boost::shared_mutex requests_lock_;

/* Reuters Wire Format versions. */
		boost::atomic_uint16_t min_rwf_version_;

/* Directory mapped ServiceID */
		boost::atomic_uint32_t service_id_;

/* Pre-allocated shared resource. */
		rfa::message::RespMsg response_;
		rfa::data::Array array_;
		rfa::data::ElementList elementList_;
		rfa::data::FilterList filterList_;
		rfa::message::AttribInfo attribInfo_;
		rfa::common::RespStatus status_;
/* Client shared resource. */
		rfa::data::Map map_;
		zmq_msg_t msg_;
		provider::Request request_;

/* Iterator for populating publish fields */
		rfa::data::SingleWriteIterator map_it_, element_it_;

/* RFA can reject new client requests whilst maintaining current connected sessions.
 */
		bool is_accepting_connections_;
		bool is_accepting_requests_;

/* Container of all item streams keyed by symbol name. */
		std::unordered_map<std::string, std::shared_ptr<item_stream_t>> directory_;
		boost::shared_mutex directory_lock_;

/* RFA request thread client. */
		std::shared_ptr<void> zmq_context_;
		std::shared_ptr<void> request_sock_, response_sock_;

/** Performance Counters **/
		boost::posix_time::ptime creation_time_, last_activity_;
		uint32_t cumulative_stats_[PROVIDER_PC_MAX];
		uint32_t snap_stats_[PROVIDER_PC_MAX];
	};

} /* namespace usagi */

#endif /* __PROVIDER_HH__ */

/* eof */