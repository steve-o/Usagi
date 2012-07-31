/* RFA provider.
 */

#ifndef __PROVIDER_HH__
#define __PROVIDER_HH__
#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <utility>

/* Boost Posix Time */
#include <boost/date_time/posix_time/posix_time.hpp>

/* Boost noncopyable base class */
#include <boost/utility.hpp>

/* RFA 7.2 */
#include <rfa/rfa.hh>

#include "rfa.hh"
#include "config.hh"
#include "deleter.hh"

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
		PROVIDER_PC_CLIENT_SESSION_REJECTED,
		PROVIDER_PC_CLIENT_SESSION_ACCEPTED,
/* marker */
		PROVIDER_PC_MAX
	};

	class client_t;

	class item_stream_t : boost::noncopyable
	{
	public:
		item_stream_t ()
		{
		}

/* Fixed name for this stream. */
		rfa::common::RFA_String rfa_name;
/* Request tokens for clients, can be more than one per client. */
		std::unordered_map<rfa::sessionLayer::RequestToken*, 
				   std::pair<std::shared_ptr<client_t>, bool>> clients;
	};

	class provider_t :
		public rfa::common::Client,
		boost::noncopyable
	{
	public:
		provider_t (const config_t& config, std::shared_ptr<rfa_t> rfa, std::shared_ptr<rfa::common::EventQueue> event_queue, std::shared_ptr<void> zmq_context);
		~provider_t();

		bool init() throw (rfa::common::InvalidConfigurationException, rfa::common::InvalidUsageException);

		bool createItemStream (const char* name, std::shared_ptr<item_stream_t> item_stream) throw (rfa::common::InvalidUsageException);
		bool send (item_stream_t& item_stream, rfa::message::RespMsg& msg, const rfa::message::AttribInfo& attribInfo) throw (rfa::common::InvalidUsageException);
		bool send (rfa::message::RespMsg& msg, rfa::sessionLayer::RequestToken& token) throw (rfa::common::InvalidUsageException);

/* RFA event callback. */
		void processEvent (const rfa::common::Event& event) override;

		uint8_t getRwfMajorVersion() const {
			return min_rwf_major_version_;
		}
		uint8_t getRwfMinorVersion() const {
			return min_rwf_minor_version_;
		}
		const char* getServiceName() const {
			return config_.service_name.c_str();
		}
		uint32_t getServiceId() const {
			return service_id_;
		}

	private:
		void processConnectionEvent (const rfa::sessionLayer::ConnectionEvent& event);
		void processOMMActiveClientSessionEvent (const rfa::sessionLayer::OMMActiveClientSessionEvent& event);
		void processOMMCmdErrorEvent (const rfa::sessionLayer::OMMCmdErrorEvent& event);

		bool rejectClientSession (const rfa::common::Handle* handle);
		bool acceptClientSession (const rfa::common::Handle* handle);
		bool eraseClientSession (rfa::common::Handle* handle);

		void getDirectoryResponse (rfa::message::RespMsg* msg, uint8_t rwf_major_version, uint8_t rwf_minor_version, const char* service_name, uint32_t filter_mask, uint8_t response_type);
		void getServiceDirectory (rfa::data::Map* map, uint8_t rwf_major_version, uint8_t rwf_minor_version, const char* service_name, uint32_t filter_mask);
		void getServiceFilterList (rfa::data::FilterList* filterList, uint8_t rwf_major_version, uint8_t rwf_minor_version, uint32_t filter_mask);
		void getServiceInformation (rfa::data::ElementList* elementList, uint8_t rwf_major_version, uint8_t rwf_minor_version);
		void getServiceCapabilities (rfa::data::Array* capabilities);
		void getServiceDictionaries (rfa::data::Array* dictionaries);
		void getServiceState (rfa::data::ElementList* elementList, uint8_t rwf_major_version, uint8_t rwf_minor_version);

		uint32_t send (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);
		uint32_t submit (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);

		void setServiceId (uint32_t service_id) {
			service_id_ = service_id;
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

		friend client_t;

/* Reuters Wire Format versions. */
		uint8_t min_rwf_major_version_;
		uint8_t min_rwf_minor_version_;

/* Directory mapped ServiceID */
		uint32_t service_id_;

/* Pre-allocated shared resource. */
		rfa::data::Map map_;
		rfa::message::AttribInfo attribInfo_;
		rfa::common::RespStatus status_;

/* Iterator for populating publish fields */
		rfa::data::SingleWriteIterator single_write_it_;

/* RFA can reject new client requests whilst maintaining current connected sessions.
 */
		bool is_accepting_connections_;
		bool is_accepting_requests_;
		bool is_muted_;

/* Container of all item streams keyed by symbol name. */
		std::unordered_map<std::string, std::weak_ptr<item_stream_t>> directory_;

/* RFA request thread client. */
		std::shared_ptr<void> zmq_context_;

/** Performance Counters **/
		boost::posix_time::ptime creation_time_, last_activity_;
		uint32_t cumulative_stats_[PROVIDER_PC_MAX];
		uint32_t snap_stats_[PROVIDER_PC_MAX];
	};

} /* namespace usagi */

#endif /* __PROVIDER_HH__ */

/* eof */