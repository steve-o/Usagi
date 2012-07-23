/* RFA provider client session.
 */

#ifndef __CLIENT_HH__
#define __CLIENT_HH__
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
		CLIENT_PC_RFA_MSGS_SENT,
		CLIENT_PC_RFA_EVENTS_RECEIVED,
		CLIENT_PC_RFA_EVENTS_DISCARDED,
		CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_RECEIVED,
		CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED,
		CLIENT_PC_REQUEST_MSGS_RECEIVED,
		CLIENT_PC_REQUEST_MSGS_DISCARDED,
		CLIENT_PC_MMT_LOGIN_RECEIVED,
		CLIENT_PC_MMT_LOGIN_VALIDATED,
		CLIENT_PC_MMT_LOGIN_MALFORMED,
		CLIENT_PC_MMT_LOGIN_REJECTED,
		CLIENT_PC_MMT_LOGIN_ACCEPTED,
		CLIENT_PC_MMT_LOGIN_RESPONSE_VALIDATED,
		CLIENT_PC_MMT_LOGIN_RESPONSE_MALFORMED,
		CLIENT_PC_MMT_DIRECTORY_REQUEST_RECEIVED,
		CLIENT_PC_MMT_DIRECTORY_VALIDATED,
		CLIENT_PC_MMT_DIRECTORY_MALFORMED,
		CLIENT_PC_MMT_DIRECTORY_SENT,
		CLIENT_PC_MMT_DICTIONARY_REQUEST_RECEIVED,
		CLIENT_PC_MMT_MARKET_PRICE_REQUEST_RECEIVED,
		CLIENT_PC_MMT_MARKET_PRICE_VALIDATED,
		CLIENT_PC_MMT_MARKET_PRICE_MALFORMED,
		CLIENT_PC_MMT_MARKET_PRICE_SENT,
/* marker */
		CLIENT_PC_MAX
	};

	class provider_t;

	class client_t :
		public rfa::common::Client,
		boost::noncopyable
	{
	public:
		client_t (provider_t& provider, const rfa::common::Handle* handle);
		~client_t();

		bool getAssociatedMetaInfo();

/* RFA event callback. */
		void processEvent (const rfa::common::Event& event);

		const rfa::common::Handle* getHandle() const {
			return handle_;
		}
		uint8_t getRwfMajorVersion() const {
			return rwf_major_version_;
		}
		uint8_t getRwfMinorVersion() const {
			return rwf_minor_version_;
		}

	private:
		void processOMMSolicitedItemEvent (const rfa::sessionLayer::OMMSolicitedItemEvent& event);
		void processReqMsg (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processLoginRequest (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processDirectoryRequest (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processDictionaryRequest (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processMarketPriceRequest (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& token);
		void processOMMItemEvent (const rfa::sessionLayer::OMMItemEvent& event);
                void processRespMsg (const rfa::message::RespMsg& msg);
                void processLoginResponse (const rfa::message::RespMsg& msg);
                void processLoginSuccess (const rfa::message::RespMsg& msg);
                void processLoginSuspect (const rfa::message::RespMsg& msg);
                void processLoginClosed (const rfa::message::RespMsg& msg);
		void processOMMCmdErrorEvent (const rfa::sessionLayer::OMMCmdErrorEvent& event);

		bool rejectLogin (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& login_token);
		bool acceptLogin (const rfa::message::ReqMsg& msg, rfa::sessionLayer::RequestToken& login_token);
		bool sendDirectoryResponse (rfa::sessionLayer::RequestToken& token);
		bool sendDirectoryResponse();
		bool sendBlankResponse (rfa::sessionLayer::RequestToken& token, const char* service_name, const char* name);
		bool sendLoginRequest() throw (rfa::common::InvalidUsageException);

		uint32_t submit (rfa::common::Msg& msg, rfa::sessionLayer::RequestToken& token, void* closure) throw (rfa::common::InvalidUsageException);

		provider_t& provider_;

/* unique id per connection. */
		std::string prefix_;

/* RFA Client Session event consumer */
		const rfa::common::Handle* handle_;

/* Reuters Wire Format versions. */
		uint8_t rwf_major_version_;
		uint8_t rwf_minor_version_;

/* RFA will return a CmdError message if the provider application submits data
 * before receiving a login success message.  Mute downstream publishing until
 * permission is granted to submit data.
 */
		bool is_muted_;

/* Last RespStatus details. */
		int stream_state_;
		int data_state_;

/** Performance Counters **/
		boost::posix_time::ptime creation_time_, last_activity_;
		uint32_t cumulative_stats_[CLIENT_PC_MAX];
		uint32_t snap_stats_[CLIENT_PC_MAX];

#ifdef STITCHMIB_H
		friend Netsnmp_Next_Data_Point stitchClientTable_get_next_data_point;
		friend Netsnmp_Node_Handler stitchClientTable_handler;

		friend Netsnmp_Next_Data_Point stitchClientPerformanceTable_get_next_data_point;
		friend Netsnmp_Node_Handler stitchClientPerformanceTable_handler;
#endif /* STITCHMIB_H */
	};

} /* namespace usagi */

#endif /* __CLIENT_HH__ */

/* eof */
