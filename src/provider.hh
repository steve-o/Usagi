/* RFA provider.
 */

#ifndef __PROVIDER_HH__
#define __PROVIDER_HH__
#pragma once

#include <cstdint>
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
		PROVIDER_PC_OMM_ITEM_EVENTS_RECEIVED,
		PROVIDER_PC_OMM_ITEM_EVENTS_DISCARDED,
		PROVIDER_PC_RESPONSE_MSGS_RECEIVED,
		PROVIDER_PC_RESPONSE_MSGS_DISCARDED,
		PROVIDER_PC_MMT_LOGIN_RESPONSE_RECEIVED,
		PROVIDER_PC_MMT_LOGIN_RESPONSE_DISCARDED,
		PROVIDER_PC_MMT_LOGIN_RESPONSE_MALFORMED,
		PROVIDER_PC_MMT_LOGIN_RESPONSE_VALIDATED,
		PROVIDER_PC_MMT_LOGIN_SUCCESS_RECEIVED,
		PROVIDER_PC_MMT_LOGIN_SUSPECT_RECEIVED,
		PROVIDER_PC_MMT_LOGIN_CLOSED_RECEIVED,
		PROVIDER_PC_OMM_CMD_ERRORS,
		PROVIDER_PC_MMT_LOGIN_VALIDATED,
		PROVIDER_PC_MMT_LOGIN_MALFORMED,
		PROVIDER_PC_MMT_LOGIN_SENT,
		PROVIDER_PC_MMT_DIRECTORY_VALIDATED,
		PROVIDER_PC_MMT_DIRECTORY_MALFORMED,
		PROVIDER_PC_MMT_DIRECTORY_SENT,
		PROVIDER_PC_CONNECTION_EVENTS_RECEIVED,
		PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED,
		PROVIDER_PC_CLIENT_SESSION_REJECTED,
		PROVIDER_PC_CLIENT_SESSION_ACCEPTED,
		PROVIDER_PC_OMM_INACTIVE_CLIENT_SESSION_RECEIVED,
		PROVIDER_PC_OMM_SOLICITED_ITEM_EVENTS_RECEIVED,
		PROVIDER_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED,
		PROVIDER_PC_REQUEST_MSGS_RECEIVED,
		PROVIDER_PC_REQUEST_MSGS_DISCARDED,
		PROVIDER_PC_MMT_LOGIN_REQUEST_RECEIVED,
		PROVIDER_PC_MMT_LOGIN_REQUEST_REJECTED,
		PROVIDER_PC_MMT_LOGIN_REQUEST_ACCEPTED,
		PROVIDER_PC_MMT_DIRECTORY_REQUEST_RECEIVED,
		PROVIDER_PC_MMT_DICTIONARY_REQUEST_RECEIVED,
		PROVIDER_PC_MMT_MARKET_PRICE_REQUEST_RECEIVED,
		PROVIDER_PC_TOKENS_GENERATED,
/* marker */
		PROVIDER_PC_MAX
	};

	class item_stream_t : boost::noncopyable
	{
	public:
		item_stream_t () :
			token (nullptr)
		{
		}

/* Fixed name for this stream. */
		rfa::common::RFA_String rfa_name;
/* Session token which is valid from login success to login close. */
		rfa::sessionLayer::ItemToken* token;
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
			return rwf_major_version_;
		}
		uint8_t getRwfMinorVersion() {
			return rwf_minor_version_;
		}

	private:
		void processConnectionEvent (const rfa::sessionLayer::ConnectionEvent& event);
		void processOMMActiveClientSessionEvent (const rfa::sessionLayer::OMMActiveClientSessionEvent& event);
		void processOMMInactiveClientSessionEvent (const rfa::sessionLayer::OMMInactiveClientSessionEvent& event);
		void processOMMItemEvent (const rfa::sessionLayer::OMMItemEvent& event);
		void processOMMSolicitedItemEvent (const rfa::sessionLayer::OMMSolicitedItemEvent& event);
		void processReqMsg (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processLoginRequest (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processDirectoryRequest (const rfa::message::ReqMsg& msg);
		void processDictionaryRequest (const rfa::message::ReqMsg& msg);
		void processMarketPriceRequest (const rfa::message::ReqMsg& msg);
                void processRespMsg (const rfa::message::RespMsg& msg);
                void processLoginResponse (const rfa::message::RespMsg& msg);
                void processLoginSuccess (const rfa::message::RespMsg& msg);
                void processLoginSuspect (const rfa::message::RespMsg& msg);
                void processLoginClosed (const rfa::message::RespMsg& msg);
		void processOMMCmdErrorEvent (const rfa::sessionLayer::OMMCmdErrorEvent& event);

		bool rejectClientSession (const rfa::common::Handle* handle);
		bool acceptClientSession (const rfa::common::Handle* handle);
		bool rejectLogin (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& login_token);
		bool acceptLogin (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& login_token);
		bool sendDirectoryResponse();
		void getServiceDirectory (rfa::data::Map& map);
		void getServiceFilterList (rfa::data::FilterList& filterList);
		void getServiceInformation (rfa::data::ElementList& elementList);
		void getServiceCapabilities (rfa::data::Array& capabilities);
		void getServiceDictionaries (rfa::data::Array& dictionaries);
		void getServiceState (rfa::data::ElementList& elementList);
		bool resetTokens();

		uint32_t send (rfa::common::Msg& msg, rfa::sessionLayer::ItemToken& token, void* closure) throw (rfa::common::InvalidUsageException);
		uint32_t submit (rfa::common::Msg& msg, rfa::sessionLayer::ItemToken& token, void* closure) throw (rfa::common::InvalidUsageException);
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
/* RFA Client Session event consumer */
		std::vector<rfa::common::Handle*> session_item_handles_;
/* RFA Error Item event consumer */
		rfa::common::Handle* error_item_handle_;
/* RFA Item event consumer */
		rfa::common::Handle* item_handle_;

/* Reuters Wire Format versions. */
		uint8_t rwf_major_version_;
		uint8_t rwf_minor_version_;

/* RFA can reject new client requests whilst maintaining current connected sessions.
 */
		bool is_muted_;

/* Last RespStatus details. */
		int stream_state_;
		int data_state_;

/* Container of all item streams keyed by symbol name. */
		std::unordered_map<std::string, std::weak_ptr<item_stream_t>> directory_;

/** Performance Counters **/
		boost::posix_time::ptime last_activity_;
		uint32_t cumulative_stats_[PROVIDER_PC_MAX];
		uint32_t snap_stats_[PROVIDER_PC_MAX];
	};

} /* namespace usagi */

#endif /* __PROVIDER_HH__ */

/* eof */