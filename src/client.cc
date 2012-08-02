/* RFA provider client session.
 */

#include "client.hh"

#include <algorithm>
#include <utility>

#include <windows.h>

/* ZeroMQ messaging middleware. */
#include <zmq.h>

#include "chromium/logging.hh"
#include "error.hh"
#include "rfaostream.hh"
#include "provider.hh"
#include "provider.pb.h"

using rfa::common::RFA_String;

usagi::client_t::client_t (
	usagi::provider_t& provider,
	const rfa::common::Handle* handle
	) :
	creation_time_ (boost::posix_time::second_clock::universal_time()),
	last_activity_ (creation_time_),
	provider_ (provider),
	handle_ (nullptr),
	login_token_ (nullptr),
	rwf_major_version_ (0),
	rwf_minor_version_ (0),
	is_muted_ (true),
	is_logged_in_ (false)
{
	ZeroMemory (cumulative_stats_, sizeof (cumulative_stats_));
	ZeroMemory (snap_stats_, sizeof (snap_stats_));

/* Set logger ID */
	std::ostringstream ss;
	ss << handle << ':';
	prefix_.assign (ss.str());
}

usagi::client_t::~client_t()
{
	using namespace boost::posix_time;
	const auto uptime = second_clock::universal_time() - creation_time_;
	VLOG(3) << prefix_ << "Summary: {"
		 " \"Uptime\": \"" << to_simple_string (uptime) << "\""
		", \"RfaEventsReceived\": " << cumulative_stats_[CLIENT_PC_RFA_EVENTS_RECEIVED] <<
		", \"RfaMessagesSent\": " << cumulative_stats_[CLIENT_PC_RFA_MSGS_SENT] <<
		" }";
}

bool
usagi::client_t::init (
	rfa::common::Handle*const handle,
	std::shared_ptr<void> zmq_context
	)
{
/* save non-const client session handle. */
	handle_ = handle;

/* create new push socket for submitting refresh requests. */
	try {
		std::function<int(void*)> zmq_close_deleter = zmq_close;
		sender_.reset (zmq_socket (zmq_context.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)sender_);
		int rc = zmq_connect (sender_.get(), "inproc://usagi/refresh");
		CHECK(0 == rc);
	} catch (std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		return false;
	}

	return true;
}

bool
usagi::client_t::getAssociatedMetaInfo()
{
	DCHECK(nullptr != handle_);

	last_activity_ = boost::posix_time::second_clock::universal_time();

/* Store negotiated Reuters Wire Format version information. */
	rfa::data::Map map;
	map.setAssociatedMetaInfo (*handle_);
	rwf_major_version_ = map.getMajorVersion();
	rwf_minor_version_ = map.getMinorVersion();
	LOG(INFO) << prefix_ <<
		"RWF: { "
		  "\"MajorVersion\": " << (unsigned)rwf_major_version_ <<
		", \"MinorVersion\": " << (unsigned)rwf_minor_version_ <<
		" }";
	return true;
}

void
usagi::client_t::processEvent (
	const rfa::common::Event& event_
	)
{
	VLOG(1) << event_;
	cumulative_stats_[CLIENT_PC_RFA_EVENTS_RECEIVED]++;
	last_activity_ = boost::posix_time::second_clock::universal_time();
	switch (event_.getType()) {
	case rfa::sessionLayer::OMMSolicitedItemEventEnum:
		processOMMSolicitedItemEvent (static_cast<const rfa::sessionLayer::OMMSolicitedItemEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMInactiveClientSessionEventEnum:
		processOMMInactiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMInactiveClientSessionEvent&>(event_));
		break;

        default:
		cumulative_stats_[CLIENT_PC_RFA_EVENTS_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Uncaught: " << event_;
                break;
        }
}

/* 7.4.7.2 Handling consumer solicited item events.
 */
void
usagi::client_t::processOMMSolicitedItemEvent (
	const rfa::sessionLayer::OMMSolicitedItemEvent&	item_event
	)
{
	cumulative_stats_[CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_RECEIVED]++;
	const rfa::common::Msg& msg = item_event.getMsg();

	if (msg.isBlank()) {
		cumulative_stats_[CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Discarding blank solicited message: " << msg;
		return;
	}

	switch (msg.getMsgType()) {
	case rfa::message::ReqMsgEnum:
		processReqMsg (static_cast<const rfa::message::ReqMsg&>(msg), item_event.getRequestToken());
		break;
	default:
		cumulative_stats_[CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Uncaught solicited message: " << msg;
		break;
	}
}

void
usagi::client_t::processReqMsg (
	const rfa::message::ReqMsg& request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_REQUEST_MSGS_RECEIVED]++;
	switch (request_msg.getMsgModelType()) {
	case rfa::rdm::MMT_LOGIN:
		processLoginRequest (request_msg, request_token);
		break;
	case rfa::rdm::MMT_DIRECTORY:
		processDirectoryRequest (request_msg, request_token);
		break;
	case rfa::rdm::MMT_DICTIONARY:
		processDictionaryRequest (request_msg, request_token);
		break;
	case rfa::rdm::MMT_MARKET_PRICE:
	case rfa::rdm::MMT_MARKET_BY_ORDER:
	case rfa::rdm::MMT_MARKET_BY_PRICE:
	case rfa::rdm::MMT_MARKET_MAKER:
	case rfa::rdm::MMT_SYMBOL_LIST:
		processItemRequest (request_msg, request_token);
		break;
	default:
		cumulative_stats_[CLIENT_PC_REQUEST_MSGS_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Uncaught: " << request_msg;
		break;
	}
}

/* The message model type MMT_LOGIN represents a login request. Specific
 * information about the user e.g., name,name type, permission information,
 * single open, etc is available from the AttribInfo in the ReqMsg accessible
 * via getAttribInfo(). The Provider is responsible for processing this
 * information to determine whether to accept the login request.
 *
 * RFA assumes default values for all attributes not specified in the Provider’s
 * login response. For example, if a provider does not specify SingleOpen
 * support in its login response, RFA assumes the provider supports it.
 *
 *   InteractionType:     Streaming request || Pause request.
 *   QualityOfServiceReq: Not used.
 *   Priority:            Not used.
 *   Header:              Not used.
 *   Payload:             Not used.
 *
 * RDM 3.4.4 Authentication: multiple logins per client session are not supported.
 */
void
usagi::client_t::processLoginRequest (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	cumulative_stats_[CLIENT_PC_MMT_LOGIN_RECEIVED]++;
/* Pass through RFA validation and report exceptions */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
/* 4.2.8 Message Validation. */
		RFA_String warningText;
		validation_status = login_msg.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(WARNING) << prefix_ << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_MALFORMED]++;
		LOG(WARNING) << prefix_ <<
			"MMT_LOGIN::InvalidUsageException: { " <<
			  "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << login_msg <<
			", \"RequestToken\": " << (intptr_t)&login_token <<
			" }";
	}

	static const uint8_t streaming_request = rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag;
	static const uint8_t pause_request     = rfa::message::ReqMsg::PauseFlag;

	try {
/* Reject on RFA validation failing. */
		if (rfa::message::MsgValidationError == validation_status) 
		{
			LOG(WARNING) << prefix_ << "Rejecting MMT_LOGIN as RFA validation failed.";
			rejectLogin (login_msg, login_token);
			return;
		}

		const bool is_streaming_request = ((login_msg.getInteractionType() == streaming_request)
						|| (login_msg.getInteractionType() == (streaming_request | pause_request)));
		const bool is_pause_request     = (login_msg.getInteractionType() == pause_request);

/* RDM 3.2.4: All message types except GenericMsg should include an AttribInfo.
 * RFA example code verifies existence of AttribInfo with an assertion.
 */
		const bool has_attribinfo = (0 != (login_msg.getHintMask() & rfa::message::ReqMsg::AttribInfoFlag));
		const bool has_name = has_attribinfo && (rfa::message::AttribInfo::NameFlag == (login_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::NameFlag));
		const bool has_nametype = has_attribinfo && (rfa::message::AttribInfo::NameTypeFlag == (login_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::NameTypeFlag));

/* invalid RDM login. */
		if ((!is_streaming_request && !is_pause_request)
			|| !has_attribinfo
			|| !has_name
			|| !has_nametype)
		{
			cumulative_stats_[CLIENT_PC_MMT_LOGIN_MALFORMED]++;
			LOG(WARNING) << prefix_ << "Rejecting MMT_LOGIN as RDM validation failed: " << login_msg;
			rejectLogin (login_msg, login_token);
		}
		else
		{
			acceptLogin (login_msg, login_token);

/* save token for closing the session. */
			login_token_ = &login_token;
			is_logged_in_ = true;
		}
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_EXCEPTION]++;
		LOG(ERROR) << prefix_ <<
			"MMT_LOGIN::InvalidUsageException: { "
			   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << login_msg <<
			", \"RequestToken\": " << (intptr_t)&login_token <<
			" }";
	}
}

/** Rejecting Login **
 * In the case where the Provider rejects the login, it should create a RespMsg
 * as above, but set the RespType and RespStatus to the reject semantics
 * specified in RFA API 7 RDM Usage Guide. The provider application should
 * populate an OMMSolicitedItemCmd with this RespMsg, set the corresponding
 * request token and call submit() on the OMM Provider.
 *
 * Once the Provider determines that the login is to be logged out (rejected),
 * it is responsible to clean up all references to request tokens for that
 * particular client session. In addition, any incoming requests that may be
 * received after the login rejection has been submitted should be ignored.
 *
 * NB: The provider application can reject a login at any time after it has
 *     accepted a particular login.
 */
bool
usagi::client_t::rejectLogin (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	VLOG(2) << prefix_ << "Sending MMT_LOGIN rejection.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;
/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_LOGIN);
/* 7.5.9.3 Set response type.  RDM 3.2.2 RespMsg: Status when rejecting login. */
	response.setRespType (rfa::message::RespMsg::StatusEnum);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	rfa::message::AttribInfo attribInfo;
/* RDM 3.2.4 AttribInfo: Name is required, NameType is recommended: default is USER_NAME (1) */
	attribInfo.setNameType (login_msg.getAttribInfo().getNameType());
	attribInfo.setName (login_msg.getAttribInfo().getName());
	response.setAttribInfo (attribInfo);

	rfa::common::RespStatus status;
/* Item interaction state: RDM 3.2.2 RespMsg: Closed or ClosedRecover. */
	status.setStreamState (rfa::common::RespStatus::ClosedEnum);
/* Data quality state: RDM 3.2.2 RespMsg: Suspect. */
	status.setDataState (rfa::common::RespStatus::SuspectEnum);
/* Error code: RDM 3.4.3 Authentication: NotAuthorized. */
	status.setStatusCode (rfa::common::RespStatus::NotAuthorizedEnum);
	response.setRespStatus (status);

/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
		RFA_String warningText;
		validation_status = response.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_RESPONSE_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(WARNING) << prefix_ << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_RESPONSE_MALFORMED]++;
		LOG(ERROR) << prefix_ <<
			"MMT_LOGIN::InvalidUsageException: { " <<
			   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << response <<
			" }";
	}

	submit (static_cast<rfa::common::Msg&> (response), login_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_LOGIN_REJECTED]++;
	return true;
}

/** Accepting Login **
 * In the case where the Provider accepts the login, it should create a RespMsg
 * with RespType and RespStatus set according to RFA API 7 RDM Usage Guide. The
 * provider application should populate an OMMSolicitedItemCmd with this
 * RespMsg, set the corresponding request token and call submit() on the OMM
 * Provider.
 *
 * NB: There can only be one login per client session.
 */
bool
usagi::client_t::acceptLogin (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	VLOG(2) << prefix_ << "Sending MMT_LOGIN accepted.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;
/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_LOGIN);
/* 7.5.9.3 Set response type.  RDM 3.2.2 RespMsg: Refresh when accepting login. */
	response.setRespType (rfa::message::RespMsg::RefreshEnum);
	response.setIndicationMask (rfa::message::RespMsg::RefreshCompleteFlag);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	rfa::message::AttribInfo attribInfo;
/* RDM 3.2.4 AttribInfo: Name is required, NameType is recommended: default is USER_NAME (1) */
	attribInfo.setNameType (login_msg.getAttribInfo().getNameType());
	attribInfo.setName (login_msg.getAttribInfo().getName());
/* RDM 3.3.2 Login Response Elements */
	rfa::data::ElementList el;
	rfa::data::ElementListWriteIterator it;
	rfa::data::ElementEntry entry;
	rfa::data::DataBuffer dataBuffer;
	it.start (el);
/* Reflect back DACS authentication parameters. */
	if (login_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::AttribFlag)
	{
/* RDM Table 52: RFA will raise a warning if request & reponse differ. */
	}
/* Images and & updates could be stale. */
	entry.setName (rfa::rdm::ENAME_ALLOW_SUSPECT_DATA);
	dataBuffer.setUInt (1);
	entry.setData (dataBuffer);
	it.bind (entry);
/* No permission expressions. */
	entry.setName (rfa::rdm::ENAME_PROV_PERM_EXP);
	dataBuffer.setUInt (0);
	entry.setData (dataBuffer);
	it.bind (entry);
/* No permission profile. */
	entry.setName (rfa::rdm::ENAME_PROV_PERM_PROF);
	dataBuffer.setUInt (0);
	entry.setData (dataBuffer);
	it.bind (entry);
/* Downstream application drives stream recovery. */
	entry.setName (rfa::rdm::ENAME_SINGLE_OPEN);
	dataBuffer.setUInt (0);
	entry.setData (dataBuffer);
	it.bind (entry);
/* Batch requests not supported. */
/* OMM posts not supported. */
/* Optimized pause and resume not supported. */
/* Views not supported. */
/* Warm standby not supported. */
/* Binding complete. */
	it.complete();
	attribInfo.setAttrib (el);
	response.setAttribInfo (attribInfo);

	rfa::common::RespStatus status;
/* Item interaction state: RDM 3.2.2 RespMsg: Open. */
	status.setStreamState (rfa::common::RespStatus::OpenEnum);
/* Data quality state: RDM 3.2.2 RespMsg: Ok. */
	status.setDataState (rfa::common::RespStatus::OkEnum);
/* Error code: RDM 3.2.2 RespMsg: None. */
	status.setStatusCode (rfa::common::RespStatus::NoneEnum);
	response.setRespStatus (status);

/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
		RFA_String warningText;
		validation_status = response.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_RESPONSE_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(WARNING) << prefix_ << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_LOGIN_RESPONSE_MALFORMED]++;
		LOG(ERROR) << prefix_ <<
			"MMT_LOGIN::InvalidUsageException: { " <<
			   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << response <<
			" }";
	}

	submit (static_cast<rfa::common::Msg&> (response), login_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_LOGIN_ACCEPTED]++;
	return true;
}

/* RDM 4.2.1 ReqMsg
 * Streaming request or Nonstreaming request. No special semantics or
 * restrictions. Pause request is not supported.
 */
void
usagi::client_t::processDirectoryRequest (
	const rfa::message::ReqMsg& request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_REQUEST_RECEIVED]++;
/* Pass through RFA validation and report exceptions */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
/* 4.2.8 Message Validation. */
		RFA_String warningText;
		validation_status = request_msg.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_REQUEST_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(WARNING) << prefix_ << "MMT_DIRECTORY::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_REQUEST_MALFORMED]++;
		LOG(WARNING) << prefix_ <<
			"MMT_DIRECTORY::InvalidUsageException: { " <<
			  "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << request_msg <<
			", \"RequestToken\": " << (intptr_t)&request_token <<
			" }";
	}

	static const uint8_t snapshot_request  = rfa::message::ReqMsg::InitialImageFlag;
	static const uint8_t streaming_request = snapshot_request | rfa::message::ReqMsg::InterestAfterRefreshFlag;

	try {
/* Reject on RFA validation failing. */
		if (rfa::message::MsgValidationError == validation_status) 
		{
			LOG(WARNING) << prefix_ << "Discarded MMT_DIRECTORY request as RFA validation failed.";
			return;
		}

/* RDM 4.2.4 AttribInfo required for ReqMsg. */
		const bool has_attribinfo = (0 != (request_msg.getHintMask() & rfa::message::ReqMsg::AttribInfoFlag));

		const bool is_snapshot_request  = (request_msg.getInteractionType() == snapshot_request);
		const bool is_streaming_request = (request_msg.getInteractionType() == streaming_request);

		if ((!is_snapshot_request && !is_streaming_request)
			|| !has_attribinfo)
		{
			cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_MALFORMED]++;
			LOG(WARNING) << prefix_ << "Discarded MMT_DIRECTORY request as RDM validation failed: " << request_msg;
			return;
		}

/* Filtering of directory contents. */
		const bool has_datamask = (0 != (request_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::DataMaskFlag));
		const uint32_t filter_mask = has_datamask ? request_msg.getAttribInfo().getDataMask() : UINT32_MAX;
/* Provides ServiceName */
		if (0 != (request_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::ServiceNameFlag))
		{
			const char* service_name = request_msg.getAttribInfo().getServiceName().c_str();
			sendDirectoryResponse (request_token, service_name, filter_mask);
		}
/* Provides ServiceID */
		else if (0 != (request_msg.getAttribInfo().getHintMask() & rfa::message::AttribInfo::ServiceIDFlag) &&
			0 != provider_.getServiceId() /* service id is unknown */)
		{
			const uint32_t service_id = request_msg.getAttribInfo().getServiceID();
			if (service_id == provider_.getServiceId()) {
				sendDirectoryResponse (request_token, provider_.getServiceName(), filter_mask);
			} else {
/* default to full directory if id does not match */
				LOG(WARNING) << prefix_ << "Received MMT_DIRECTORY request for unknown service id #" << service_id << ", returning entire directory.";
				sendDirectoryResponse (request_token, nullptr, filter_mask);
			}
		}
/* Provide all services directory. */
		else
		{
			sendDirectoryResponse (request_token, nullptr, filter_mask);
		}
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_EXCEPTION]++;
		LOG(ERROR) << prefix_ << "MMT_DIRECTORY::InvalidUsageException: { "
				"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
}

void
usagi::client_t::processDictionaryRequest (
	const rfa::message::ReqMsg&	request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_MMT_DICTIONARY_REQUEST_RECEIVED]++;
	LOG(INFO) << prefix_ << "DictionaryRequest:" << request_msg;
}

void
usagi::client_t::processItemRequest (
	const rfa::message::ReqMsg&	request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_ITEM_REQUEST_RECEIVED]++;
	LOG(INFO) << prefix_ << "ItemRequest:" << request_msg;

/* 10.3.6 Handling Item Requests
 * - Ensure that the requesting session is logged in.
 * - Determine whether the requested QoS can be satisified.
 * - Ensure that the same stream is not already provisioned.
 */
	static const uint8_t streaming_request = rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag;
	static const uint8_t snapshot_request  = rfa::message::ReqMsg::InitialImageFlag;
	static const uint8_t pause_request     = rfa::message::ReqMsg::PauseFlag;
	static const uint8_t resume_request    = rfa::message::ReqMsg::InterestAfterRefreshFlag;
	static const uint8_t close_request     = 0;

/* A response is not required to be immediately generated, for example
 * forwarding the clients request to an upstream resource and waiting for
 * a reply.
 */
	try {
		const uint32_t service_id    = request_msg.getAttribInfo().getServiceID();
		const uint8_t  model_type    = request_msg.getMsgModelType();
		const char*    item_name     = request_msg.getAttribInfo().getName().c_str();
		const size_t   item_name_len = request_msg.getAttribInfo().getName().size();
		const bool use_attribinfo_in_updates = (0 != (request_msg.getIndicationMask() & rfa::message::ReqMsg::AttribInfoInUpdatesFlag));

		if (!is_logged_in_) {
			cumulative_stats_[CLIENT_PC_ITEM_REQUEST_REJECTED]++;
			LOG(INFO) << prefix_ << "Closing request for client without accepted login.";
			sendClose (request_token, service_id, model_type, item_name, use_attribinfo_in_updates, rfa::common::RespStatus::NotAuthorizedEnum);
			return;
		}

/* Only accept MMT_MARKET_PRICE. */
		if (rfa::rdm::MMT_MARKET_PRICE != model_type)
		{
			cumulative_stats_[CLIENT_PC_ITEM_REQUEST_MALFORMED]++;
			LOG(INFO) << prefix_ << "Closing request for unsupported message model type.";
			sendClose (request_token, service_id, model_type, item_name, use_attribinfo_in_updates, rfa::common::RespStatus::NotAuthorizedEnum);
			return;
		}

		const bool is_streaming_request = (request_msg.getInteractionType() == streaming_request);
/* 7.4.3.2 Request Tokens
 * Providers should not attempt to submit data after the provider has received a close request for an item. */
		const bool is_close             = (request_msg.getInteractionType() == close_request);

/* check for request token in client watchlist. */
		auto it = items_.find (&request_token);
		if (it != items_.end())
		{
/* existing request. */
			if (is_close)
			{
/* remove from item client list. */
				cumulative_stats_[CLIENT_PC_ITEM_CLOSE_REQUEST_RECEIVED]++;
				LOG(INFO) << prefix_ << "Closing open request.";
				auto sp = it->second.lock();
				if ((bool)sp) {
					auto stream = sp.get();
					boost::upgrade_lock<boost::shared_mutex> lock (stream->lock);
					auto it = stream->requests.find (&request_token);
					DCHECK (it != stream->requests.end());
					boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock (lock);
					stream->requests.erase (it);
				}
/* remove from client item list. */
				items_.erase (it);
			}
			else if (!is_streaming_request)
			{
/* invalid. */
				cumulative_stats_[CLIENT_PC_ITEM_REQUEST_MALFORMED]++;
				LOG(INFO) << prefix_ << "Closing open request on invalid snapshot reissue request.";
				sendClose (request_token, service_id, model_type, item_name, use_attribinfo_in_updates, rfa::common::RespStatus::NotAuthorizedEnum);
			}
			else
			{
/* RDM 2.5.2 Reissue Requests
 * A consumer application can request a new refresh and change certain
 * parameters on an already requested stream.
 */
				cumulative_stats_[CLIENT_PC_ITEM_REISSUE_REQUEST_RECEIVED]++;
				LOG(INFO) << prefix_ << "Sending refresh on reissue request.";
				provider::Request request;
				zmq_msg_t msg;
				request.set_msg_type (provider::Request::MSG_REFRESH);
				request.mutable_refresh()->set_token ((uintptr_t)&request_token);
				request.mutable_refresh()->set_service_id (service_id);
				request.mutable_refresh()->set_model_type (model_type);
				request.mutable_refresh()->set_item_name (item_name, item_name_len);
				zmq_msg_init_size (&msg, request.ByteSize());
				request.SerializeToArray (zmq_msg_data (&msg), (int)zmq_msg_size (&msg));
				zmq_send (sender_.get(), &msg, 0);
				zmq_msg_close (&msg);
			}
		}
		else
		{
/* capture ServiceID */
			if (0 == provider_.getServiceId()
			    && 0 == request_msg.getAttribInfo().getServiceName().compareCase (provider_.getServiceName()))
			{
				LOG(INFO) << prefix_ << "Detected service id #" << service_id << " for \"" << provider_.getServiceName() << "\".";
				provider_.setServiceId (service_id);
			}

/* new request. */
			if (is_close)
			{
/* invalid. */
				cumulative_stats_[CLIENT_PC_ITEM_REQUEST_MALFORMED]++;
				LOG(INFO) << prefix_ << "Discarding close request on closed item.";
			}
			else if (!is_streaming_request)
			{
/* closest equivalent to not-supported is NotAuthorizedEnum. */
				cumulative_stats_[CLIENT_PC_ITEM_REQUEST_MALFORMED]++;
				LOG(INFO) << prefix_ << "Closing unsupported snapshot request.";
				sendClose (request_token, service_id, model_type, item_name, use_attribinfo_in_updates, rfa::common::RespStatus::NotAuthorizedEnum);
			}
			else
			{
/* check for item in inventory */
				boost::shared_lock<boost::shared_mutex> directory_lock (provider_.directory_lock_);
				auto it = provider_.directory_.find (item_name);
				if (it == provider_.directory_.end()) {
					cumulative_stats_[CLIENT_PC_ITEM_NOT_FOUND]++;
					LOG(INFO) << prefix_ << "Closing request for item not found in directory.";
					sendClose (request_token, service_id, model_type, item_name, use_attribinfo_in_updates, rfa::common::RespStatus::NotFoundEnum);
					return;
				}
				LOG(INFO) << prefix_ << "Sending refresh on request.";
				auto& stream = it->second;
				DCHECK ((bool)stream);
				items_.emplace (std::make_pair (&request_token, stream));
				auto client_request = std::make_shared<request_t> (stream, shared_from_this(), use_attribinfo_in_updates);
				CHECK((bool)client_request);
				boost::unique_lock<boost::shared_mutex> requests_lock (provider_.requests_lock_);
				boost::unique_lock<boost::shared_mutex> stream_lock (stream->lock);
				provider_.requests_.emplace (std::make_pair (&request_token, client_request));
				stream->requests.emplace (std::make_pair (&request_token, client_request));
/* forward request to worker pool */
				provider::Request request;
				zmq_msg_t msg;
				request.set_msg_type (provider::Request::MSG_REFRESH);
				request.mutable_refresh()->set_token ((uintptr_t)&request_token);
				request.mutable_refresh()->set_service_id (service_id);
				request.mutable_refresh()->set_model_type (model_type);
				request.mutable_refresh()->set_item_name (item_name, item_name_len);
				zmq_msg_init_size (&msg, request.ByteSize());
				request.SerializeToArray (zmq_msg_data (&msg), (int)zmq_msg_size (&msg));
				zmq_send (sender_.get(), &msg, 0);
				zmq_msg_close (&msg);
			}
		}
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_ITEM_EXCEPTION]++;
		LOG(ERROR) << prefix_ << "InvalidUsageException: { "
				   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\"" <<
				", " << request_msg <<
				", \"RequestToken\": " << (intptr_t)&request_token <<
				" }";
	}
}

/* 7.4.7.1.2 Handling Consumer Client Session Events: Client session connection
 *           has been lost.
 *
 * When the provider receives this event it should stop sending any data to that
 * client session. Then it should remove references to the client session handle
 * and its associated request tokens.
 */
void
usagi::client_t::processOMMInactiveClientSessionEvent (
	const rfa::sessionLayer::OMMInactiveClientSessionEvent& session_event
	)
{
	DCHECK(nullptr != handle_);
	cumulative_stats_[CLIENT_PC_OMM_INACTIVE_CLIENT_SESSION_RECEIVED]++;
	try {
/* reject new item requests. */
		is_logged_in_ = false;
/* remove requests from item streams. */
		VLOG(2) << prefix_ << "Removing client from " << items_.size() << " item streams.";
		std::for_each (items_.begin(), items_.end(),
			[&](const std::pair<rfa::sessionLayer::RequestToken*, std::weak_ptr<item_stream_t>>& item)
		{
			auto sp = item.second.lock();
			if (!(bool)sp)
				return;
			boost::upgrade_lock<boost::shared_mutex> lock (sp->lock);
			auto it = sp->requests.find (item.first);
			DCHECK(sp->requests.end() != it);
			boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock (lock);
			sp->requests.erase (it);
			VLOG(2) << prefix_ << sp->rfa_name;
		});
/* forward upstream to remove reference to this. */
		provider_.eraseClientSession (handle_);
/* handle is now invalid. */
		handle_ = nullptr;
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_OMM_INACTIVE_CLIENT_SESSION_EXCEPTION]++;
		LOG(ERROR) << prefix_ << "OMMInactiveClientSession::InvalidUsageException: { "
				"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
}

/* 10.3.4 Providing Service Directory (Interactive)
 * A Consumer typically requests a Directory from a Provider to retrieve
 * information about available services and their capabilities, and it is the
 * responsibility of the Provider to encode and supply the directory.
 */
bool
usagi::client_t::sendDirectoryResponse (
	rfa::sessionLayer::RequestToken& request_token,
	const char* service_name,
	uint32_t filter_mask
	)
{
	VLOG(2) << prefix_ << "Sending directory response.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;
	provider_.getDirectoryResponse (&response, rwf_major_version_, rwf_minor_version_, service_name, filter_mask, rfa::rdm::REFRESH_SOLICITED);

/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try { 
		RFA_String warningText;
		validation_status = response.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(ERROR) << prefix_ << "MMT_DIRECTORY::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_MALFORMED]++;
		LOG(ERROR) << prefix_ <<
			"MMT_DIRECTORY::InvalidUsageException: { " <<
			   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << response <<
			" }";
	}

/* Create and throw away first token for MMT_DIRECTORY. */
	submit (static_cast<rfa::common::Msg&> (response), request_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_SENT]++;
	return true;
}

bool
usagi::client_t::sendClose (
	rfa::sessionLayer::RequestToken& request_token,
	uint32_t service_id,
	uint8_t model_type,
	const char* name_c,
	bool use_attribinfo_in_updates,
	uint8_t status_code
	)
{
	VLOG(2) << prefix_ << "Sending item close { "
		  "\"RequestToken\": " << (intptr_t)&request_token <<
		", \"ServiceID\": " << service_id <<
		", \"MsgModelType\": " << (int)model_type <<
		", \"Name\": \"" << name_c << "\""
		", \"AttribInfoInUpdates\": " << (use_attribinfo_in_updates ? "true" : "false") <<
		", \"StatusCode\": " << (int)status_code <<
		" }";
/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response (false);	/* reference */

/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (model_type);
/* 7.5.9.3 Set response type. */
	response.setRespType (rfa::message::RespMsg::StatusEnum);

/* RDM 6.2.3 AttribInfo
 * if the ReqMsg set AttribInfoInUpdates, then the AttribInfo must be provided for all
 * Refresh, Status, and Update RespMsgs.
 */
	if (use_attribinfo_in_updates) {
/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
		rfa::message::AttribInfo attribInfo;
		attribInfo.setNameType (rfa::rdm::INSTRUMENT_NAME_RIC);
		const RFA_String name (name_c, 0, false);	/* reference */
		attribInfo.setServiceID (service_id);
		attribInfo.setName (name);
		response.setAttribInfo (attribInfo);
	}
	
	rfa::common::RespStatus status;
/* Item interaction state: Open, Closed, ClosedRecover, Redirected, NonStreaming, or Unspecified. */
	status.setStreamState (rfa::common::RespStatus::ClosedEnum);
/* Data quality state: Ok, Suspect, or Unspecified. */
	status.setDataState (rfa::common::RespStatus::OkEnum);
/* Error code, e.g. NotFound, InvalidArgument, ... */
	status.setStatusCode (rfa::common::RespStatus::NotFoundEnum);
	response.setRespStatus (status);

#ifdef DEBUG
/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
		RFA_String warningText;
		validation_status = response.validateMsg (&warningText);
		cumulative_stats_[CLIENT_PC_ITEM_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(ERROR) << prefix_ << "validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_ITEM_MALFORMED]++;
		LOG(ERROR) << prefix_ <<
			"InvalidUsageException: { " <<
			   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", " << response <<
			" }";
	}
#endif

	submit (static_cast<rfa::common::Msg&> (response), request_token, nullptr);	
	cumulative_stats_[CLIENT_PC_ITEM_CLOSED]++;
	return true;
}

/* Forward submit requests to containing provider.
 */
uint32_t
usagi::client_t::submit (
	rfa::common::Msg& msg,
	rfa::sessionLayer::RequestToken& token,
	void* closure
	)
{
	return provider_.submit (msg, token, closure);
}

/* eof */