/* RFA provider client session.
 */

#include "client.hh"

#include <algorithm>
#include <utility>

#include <windows.h>

#include "chromium/logging.hh"
#include "error.hh"
#include "rfaostream.hh"
#include "provider.hh"

/* RDM Usage Guide: Section 6.5: Enterprise Platform
 * For future compatibility, the DictionaryId should be set to 1 by providers.
 * The DictionaryId for the RDMFieldDictionary is 1.
 */
static const int kDictionaryId = 1;

/* RDM: Absolutely no idea. */
static const int kFieldListId = 3;

/* RDM Field Identifiers. */
static const int kRdmRdnDisplayId = 2;		/* RDNDISPLAY */

using rfa::common::RFA_String;

usagi::client_t::client_t (
	usagi::provider_t& provider,
	const rfa::common::Handle* handle
	) :
	creation_time_ (boost::posix_time::second_clock::universal_time()),
	last_activity_ (creation_time_),
	provider_ (provider),
	handle_ (handle),
	rwf_major_version_ (0),
	rwf_minor_version_ (0),
	is_muted_ (true),
	stream_state_ (0),
	data_state_ (0)
{
	ZeroMemory (cumulative_stats_, sizeof (cumulative_stats_));
	ZeroMemory (snap_stats_, sizeof (snap_stats_));

/* Set logger ID */
	std::ostringstream ss;
	ss << handle_ << ':';
	prefix_.assign (ss.str());
}

usagi::client_t::~client_t()
{
	using namespace boost::posix_time;
	auto uptime = second_clock::universal_time() - creation_time_;
	VLOG(3) << prefix_ << ": Summary: {"
		 " \"Uptime\": \"" << to_simple_string (uptime) << "\""
		", \"RfaEventsReceived\": " << cumulative_stats_[CLIENT_PC_RFA_EVENTS_RECEIVED] <<
		", \"RfaMessagesSent\": " << cumulative_stats_[CLIENT_PC_RFA_MSGS_SENT] <<
		" }";
}
	
bool
usagi::client_t::getAssociatedMetaInfo()
{
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

	switch (msg.getMsgType()) {
	case rfa::message::ReqMsgEnum:
		processReqMsg (static_cast<const rfa::message::ReqMsg&>(msg), item_event.getRequestToken());
		break;
	default:
		cumulative_stats_[CLIENT_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED]++;
		LOG(WARNING) << prefix_ << "Uncaught: " << msg;
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
		processMarketPriceRequest (request_msg, request_token);
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
			   login_msg <<
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			" }";
	}

	try {
		if (rfa::message::MsgValidationError == validation_status) 
		{
			rejectLogin (login_msg, login_token);
			return;
		}

/* RDM 3.2.4: All message types except GenericMsg should include an AttribInfo.
 * RFA example code verifies existence of AttribInfo with an assertion.
 */
		CHECK (login_msg.getHintMask() & rfa::message::ReqMsg::AttribInfoFlag);

		const uint8_t streaming_request = rfa::message::ReqMsg::InitialImageFlag | rfa::message::ReqMsg::InterestAfterRefreshFlag;
		const uint8_t pause_request     = rfa::message::ReqMsg::PauseFlag;
		const bool is_streaming_request = ((login_msg.getInteractionType() == streaming_request)
						|| (login_msg.getInteractionType() == (streaming_request | pause_request)));
		const bool is_pause_request     = (login_msg.getInteractionType() == pause_request);


		if (!is_streaming_request && !is_pause_request)
		{
			rejectLogin (login_msg, login_token);
		}
		else
		{
			acceptLogin (login_msg, login_token);
		}
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << prefix_ <<
			"MMT_LOGIN::InvalidUsageException: { "
			"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
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
	VLOG(2) << prefix_ << "Rejecting MMT_LOGIN request.";

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
			   response <<
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
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
	VLOG(2) << prefix_ << "Accepting MMT_LOGIN request.";

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
			   response <<
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			" }";
	}

	submit (static_cast<rfa::common::Msg&> (response), login_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_LOGIN_ACCEPTED]++;
	return true;
}

void
usagi::client_t::processDirectoryRequest (
	const rfa::message::ReqMsg& request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_REQUEST_RECEIVED]++;
	try {
		sendDirectoryResponse (request_token);
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
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
usagi::client_t::processMarketPriceRequest (
	const rfa::message::ReqMsg&	request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[CLIENT_PC_MMT_MARKET_PRICE_REQUEST_RECEIVED]++;
	LOG(INFO) << prefix_ << "MarketPriceRequest:" << request_msg;

	// handle = request_token.getHandle();

/* A response is not required to be immediately generated, for example
 * forwarding the clients request to an upstream resource and waiting for
 * a reply.
 */
	try {
		auto& attribInfo = request_msg.getAttribInfo();
		sendBlankResponse (request_token,
				   attribInfo.getServiceName().c_str(),
				   attribInfo.getName().c_str());
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << prefix_ << "MMT_MARKET_PRICE::InvalidUsageException: { "
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
	rfa::sessionLayer::RequestToken& request_token
	)
{
	VLOG(2) << prefix_ << "Sending directory response.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;
	provider_.getDirectoryResponse (&response, rfa::rdm::REFRESH_SOLICITED);

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
			   response <<
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			" }";
	}

/* Create and throw away first token for MMT_DIRECTORY. */
	submit (static_cast<rfa::common::Msg&> (response), request_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_DIRECTORY_SENT]++;
	return true;
}

bool
usagi::client_t::sendBlankResponse (
	rfa::sessionLayer::RequestToken& request_token,
	const char* service_name_c,
	const char* name_c
	)
{
	VLOG(2) << "Sending blank item response.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response (false);	/* reference */

/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_MARKET_PRICE);
/* 7.5.9.3 Set response type. */
	response.setRespType (rfa::message::RespMsg::RefreshEnum);
	response.setIndicationMask (rfa::message::RespMsg::RefreshCompleteFlag);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	rfa::message::AttribInfo attribInfo;
	attribInfo.setNameType (rfa::rdm::INSTRUMENT_NAME_RIC);
	RFA_String service_name (service_name_c, 0, false),	/* reference */
		name (name_c, 0, false);
	attribInfo.setServiceName (service_name);
	attribInfo.setName (name);
	LOG(INFO) << "Publishing to stream " << name;
	response.setAttribInfo (attribInfo);

/* 4.3.1 RespMsg.Payload */
// not std::map :(  derived from rfa::common::Data
	rfa::data::FieldList fields;
	fields.setAssociatedMetaInfo (getRwfMajorVersion(), getRwfMinorVersion());
	fields.setInfo (kDictionaryId, kFieldListId);
/* Set a reference to field list, not a copy */
	response.setPayload (fields);

	rfa::common::RespStatus status;
/* Item interaction state: Open, Closed, ClosedRecover, Redirected, NonStreaming, or Unspecified. */
	status.setStreamState (rfa::common::RespStatus::OpenEnum);
/* Data quality state: Ok, Suspect, or Unspecified. */
	status.setDataState (rfa::common::RespStatus::OkEnum);
/* Error code, e.g. NotFound, InvalidArgument, ... */
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
		cumulative_stats_[CLIENT_PC_MMT_MARKET_PRICE_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(ERROR) << prefix_ << "MMT_MARKET_PRICE::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[CLIENT_PC_MMT_MARKET_PRICE_MALFORMED]++;
		LOG(ERROR) << prefix_ <<
			"MMT_MARKET_PRICE::InvalidUsageException: { " <<
			   response <<
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			" }";
	}

/* Create and throw away first token for MMT_DIRECTORY. */
	submit (static_cast<rfa::common::Msg&> (response), request_token, nullptr);
	cumulative_stats_[CLIENT_PC_MMT_MARKET_PRICE_SENT]++;
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