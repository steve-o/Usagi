/* RFA provider.
 */

#ifndef __PROVIDER_HH__
#define __PROVIDER_HH__
#pragma once

#include <cstdint>
#include <forward_list>
#include <unordered_map>

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
		PROVIDER_PC_OMM_INACTIVE_CLIENT_SESSION_RECEIVED,
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
		std::unordered_map<rfa::sessionLayer::RequestToken*, client_t*> clients;
	};

	class provider_t :
		public rfa::common::Client,
		boost::noncopyable
	{
	public:
		provider_t (const config_t& config, std::shared_ptr<rfa_t> rfa, std::shared_ptr<rfa::common::EventQueue> event_queue);
		~provider_t();

		bool init() throw (rfa::common::InvalidConfigurationException, rfa::common::InvalidUsageException);

		bool createItemStream (const char* name, std::shared_ptr<item_stream_t> item_stream) throw (rfa::common::InvalidUsageException);
		bool send (item_stream_t& item_stream, rfa::common::Msg& msg) throw (rfa::common::InvalidUsageException);

/* RFA event callback. */
		void processEvent (const rfa::common::Event& event) override;

		uint8_t getRwfMajorVersion() {
			return min_rwf_major_version_;
		}
		uint8_t getRwfMinorVersion() {
			return min_rwf_minor_version_;
		}

	private:
		void processConnectionEvent (const rfa::sessionLayer::ConnectionEvent& event);
		void processOMMActiveClientSessionEvent (const rfa::sessionLayer::OMMActiveClientSessionEvent& event);
		void processOMMInactiveClientSessionEvent (const rfa::sessionLayer::OMMInactiveClientSessionEvent& event);
		void processOMMCmdErrorEvent (const rfa::sessionLayer::OMMCmdErrorEvent& event);

		bool rejectClientSession (const rfa::common::Handle* handle);
		bool acceptClientSession (const rfa::common::Handle* handle);

		void getDirectoryResponse (rfa::message::RespMsg* msg, uint8_t response_type);
		void getServiceDirectory (rfa::data::Map* map);
		void getServiceFilterList (rfa::data::FilterList* filterList);
		void getServiceInformation (rfa::data::ElementList* elementList);
		void getServiceCapabilities (rfa::data::Array* capabilities);
		void getServiceDictionaries (rfa::data::Array* dictionaries);
		void getServiceState (rfa::data::ElementList* elementList);

		uint32_t send (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);
		uint32_t submit (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);

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

/* RFA Client Sessions */
		std::vector<std::shared_ptr<client_t>> clients_;

		friend client_t;

/* Reuters Wire Format versions. */
		uint8_t min_rwf_major_version_;
		uint8_t min_rwf_minor_version_;

/* Pre-allocated shared resource. */
		rfa::data::Map map_;
		rfa::message::AttribInfo attribInfo_;
		rfa::common::RespStatus status_;

/* RFA can reject new client requests whilst maintaining current connected sessions.
 */
		bool is_accepting_connections_;
		bool is_accepting_requests_;
		bool is_muted_;

/* Last RespStatus details. */
		int stream_state_;
		int data_state_;

/* Container of all item streams keyed by symbol name. */
		std::unordered_map<std::string, std::weak_ptr<item_stream_t>> directory_;

/** Performance Counters **/
		boost::posix_time::ptime creation_time_, last_activity_;
		uint32_t cumulative_stats_[PROVIDER_PC_MAX];
		uint32_t snap_stats_[PROVIDER_PC_MAX];
	};

} /* namespace usagi */

#endif /* __PROVIDER_HH__ */

/* eof */