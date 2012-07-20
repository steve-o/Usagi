/* RFA provider.
 *
 * One single provider, and hence wraps a RFA session for simplicity.
 */

#include "provider.hh"

#include <algorithm>
#include <utility>

#include <windows.h>

#include "chromium/logging.hh"
#include "error.hh"
#include "rfaostream.hh"

using rfa::common::RFA_String;

/* 7.2.1 Configuring the Session Layer Package.
 * The config database is specified by the context name which must be "RFA".
 */
static const RFA_String kContextName ("RFA");

/* Reuters Wire Format nomenclature for dictionary names. */
static const RFA_String kRdmFieldDictionaryName ("RWFFld");
static const RFA_String kEnumTypeDictionaryName ("RWFEnum");

usagi::provider_t::provider_t (
	const usagi::config_t& config,
	std::shared_ptr<usagi::rfa_t> rfa,
	std::shared_ptr<rfa::common::EventQueue> event_queue
	) :
	config_ (config),
	rfa_ (rfa),
	event_queue_ (event_queue),
	error_item_handle_ (nullptr),
	item_handle_ (nullptr),
	rwf_major_version_ (0),
	rwf_minor_version_ (0),
	is_muted_ (false)
{
	ZeroMemory (cumulative_stats_, sizeof (cumulative_stats_));
	ZeroMemory (snap_stats_, sizeof (snap_stats_));
}

usagi::provider_t::~provider_t()
{
	VLOG(3) << "Unregistering RFA session clients.";
	if (nullptr != item_handle_)
		omm_provider_->unregisterClient (item_handle_), item_handle_ = nullptr;
	if (nullptr != error_item_handle_)
		omm_provider_->unregisterClient (error_item_handle_), error_item_handle_ = nullptr;
	omm_provider_.reset();
	session_.reset();
}

bool
usagi::provider_t::init()
{
	last_activity_ = boost::posix_time::microsec_clock::universal_time();

/* 7.2.1 Configuring the Session Layer Package.
 */
	VLOG(3) << "Acquiring RFA session.";
	const RFA_String sessionName (config_.session_name.c_str(), 0, false);
	session_.reset (rfa::sessionLayer::Session::acquire (sessionName));
	if (!(bool)session_)
		return false;

/* 6.2.2.1 RFA Version Info.  The version is only available if an application
 * has acquired a Session (i.e., the Session Layer library is loaded).
 */
	if (!rfa_->VerifyVersion())
		return false;

/* 7.4.5 Initializing an OMM Interactive Provider. */
	VLOG(3) << "Creating OMM provider.";
	const RFA_String publisherName (config_.publisher_name.c_str(), 0, false);
	omm_provider_.reset (session_->createOMMProvider (publisherName, nullptr));
	if (!(bool)omm_provider_)
		return false;

/* 7.4.6 Registering for Events from an OMM Interactive Provider. */
/* connection events. */
	VLOG(3) << "Registering connection interest.";
	rfa::sessionLayer::OMMListenerConnectionIntSpec ommListenerConnectionIntSpec;
/* (optional) specify specific connection names (comma delimited) to filter incoming events */
	connection_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommListenerConnectionIntSpec, *this, nullptr /* closure */);
	if (nullptr == connection_item_handle_)
		return false;

/* listen events. */
	VLOG(3) << "Registering listen interest.";
	rfa::sessionLayer::OMMClientSessionListenerIntSpec ommClientSessionListenerIntSpec;
	listen_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommClientSessionListenerIntSpec, *this, nullptr /* closure */);
	if (nullptr == listen_item_handle_)
		return false;

/* receive error events (OMMCmdErrorEvent) related to calls to submit(). */
	VLOG(3) << "Registering OMM error interest.";
	rfa::sessionLayer::OMMErrorIntSpec ommErrorIntSpec;
	error_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommErrorIntSpec, *this, nullptr /* closure */);
	if (nullptr == error_item_handle_)
		return false;

	return true;
}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
bool
usagi::provider_t::createItemStream (
	const char* name,
	std::shared_ptr<item_stream_t> item_stream
	)
{
	VLOG(4) << "Creating item stream for RIC \"" << name << "\".";
	item_stream->rfa_name.set (name, 0, true);
	if (!is_muted_) {
		DVLOG(4) << "Generating token for " << name;
		item_stream->token = &( omm_provider_->generateItemToken() );
		assert (nullptr != item_stream->token);
		cumulative_stats_[PROVIDER_PC_TOKENS_GENERATED]++;
	} else {
		DVLOG(4) << "Not generating token for " << name << " as provider is muted.";
		assert (nullptr == item_stream->token);
	}
	const std::string key (name);
	auto status = directory_.emplace (std::make_pair (key, item_stream));
	assert (true == status.second);
	assert (directory_.end() != directory_.find (key));
	DVLOG(4) << "Directory size: " << directory_.size();
	last_activity_ = boost::posix_time::microsec_clock::universal_time();
	return true;
}

/* Send an Rfa message through the pre-created item stream.
 */

bool
usagi::provider_t::send (
	item_stream_t& item_stream,
	rfa::common::Msg& msg
)
{
	if (is_muted_)
		return false;
	assert (nullptr != item_stream.token);
	send (msg, *item_stream.token, nullptr);
	cumulative_stats_[PROVIDER_PC_MSGS_SENT]++;
	last_activity_ = boost::posix_time::microsec_clock::universal_time();
	return true;
}

uint32_t
usagi::provider_t::send (
	rfa::common::Msg& msg,
	rfa::sessionLayer::ItemToken& token,
	void* closure
	)
{
	return submit (msg, token, closure);
}

/* 7.5.9.6 Create the OMMItemCmd object and populate it with the response
 * message.  The Cmd essentially acts as a wrapper around the response message.
 * The Cmd may be created on the heap or the stack.
 */
uint32_t
usagi::provider_t::submit (
	rfa::common::Msg& msg,
	rfa::sessionLayer::ItemToken& token,
	void* closure
	)
{
	rfa::sessionLayer::OMMItemCmd itemCmd;
	itemCmd.setMsg (msg);
/* 7.5.9.7 Set the unique item identifier. */
	itemCmd.setItemToken (&token);
/* 7.5.9.8 Write the response message directly out to the network through the
 * connection.
 */
	assert ((bool)omm_provider_);
	const uint32_t submit_status = omm_provider_->submit (&itemCmd, closure);
	cumulative_stats_[PROVIDER_PC_RFA_MSGS_SENT]++;
	return submit_status;
}

/* 7.4.8 Sending response messages using an OMM provider.
 */
uint32_t
usagi::provider_t::submit (
	rfa::common::Msg& msg,
	rfa::sessionLayer::RequestToken& token,
	void* closure
	)
{
	rfa::sessionLayer::OMMSolicitedItemCmd itemCmd;
	itemCmd.setMsg (msg);
/* 7.5.9.7 Set the unique item identifier. */
	itemCmd.setRequestToken (token);
/* 7.5.9.8 Write the response message directly out to the network through the
 * connection.
 */
	assert ((bool)omm_provider_);
	const uint32_t submit_status = omm_provider_->submit (&itemCmd, closure);
	cumulative_stats_[PROVIDER_PC_RFA_MSGS_SENT]++;
	return submit_status;
}

/* Entry point for all RFA events subscribed via a RegisterClient() API.
 */
void
usagi::provider_t::processEvent (
	const rfa::common::Event& event_
	)
{
	VLOG(1) << event_;
	cumulative_stats_[PROVIDER_PC_RFA_EVENTS_RECEIVED]++;
	switch (event_.getType()) {
	case rfa::sessionLayer::ConnectionEventEnum:
		processConnectionEvent(static_cast<const rfa::sessionLayer::ConnectionEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMActiveClientSessionEventEnum:
		processOMMActiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMActiveClientSessionEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMInactiveClientSessionEventEnum:
		processOMMInactiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMInactiveClientSessionEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMItemEventEnum:
		processOMMItemEvent (static_cast<const rfa::sessionLayer::OMMItemEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMSolicitedItemEventEnum:
		processOMMSolicitedItemEvent (static_cast<const rfa::sessionLayer::OMMSolicitedItemEvent&>(event_));
		break;

        case rfa::sessionLayer::OMMCmdErrorEventEnum:
                processOMMCmdErrorEvent (static_cast<const rfa::sessionLayer::OMMCmdErrorEvent&>(event_));
                break;

        default:
		cumulative_stats_[PROVIDER_PC_RFA_EVENTS_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << event_;
                break;
        }
}

/* 7.4.7.4 Handling Listener Connection Events (new connection events).
 */
void
usagi::provider_t::processConnectionEvent (
	const rfa::sessionLayer::ConnectionEvent& connection_event
	)
{
	cumulative_stats_[PROVIDER_PC_CONNECTION_EVENTS_RECEIVED]++;
}

/* 7.4.7.1.1 Handling Consumer Client Session Events: New client session request.
 *
 * There are many reasons why a provider might reject a connection. For
 * example, it might have a maximum supported number of connections.
 */
void
usagi::provider_t::processOMMActiveClientSessionEvent (
	const rfa::sessionLayer::OMMActiveClientSessionEvent& session_event
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED]++;
	try {
		const auto handle = session_event.getClientSessionHandle();
		if (is_muted_)
		{
			rejectClientSession (handle);
		}
		else
		{
			acceptClientSession (handle);
		}
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "OMMActiveClientSession::InvalidUsageException: { "
				"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
}

bool
usagi::provider_t::rejectClientSession (
	const rfa::common::Handle* handle
	)
{
	VLOG(2) << "Rejecting new client session request.";
		
/* 7.4.10.2 Closing down a client session. */
	rfa::sessionLayer::OMMClientSessionCmd rejectCmd;
	rejectCmd.setClientSessionHandle (handle);
	rfa::sessionLayer::ClientSessionStatus status;
	status.setState (rfa::sessionLayer::ClientSessionStatus::Inactive);
	status.setStatusCode (rfa::sessionLayer::ClientSessionStatus::Reject);
	rejectCmd.setStatus (status);

	const uint32_t submit_status = omm_provider_->submit (&rejectCmd, nullptr);
	cumulative_stats_[PROVIDER_PC_CLIENT_SESSION_REJECTED]++;
	return true;
}

bool
usagi::provider_t::acceptClientSession (
	const rfa::common::Handle* handle
	)
{
	VLOG(2) << "Accepting new client session request.";

/* 7.4.7.2.1 Handling login requests. */
	rfa::sessionLayer::OMMClientSessionIntSpec ommClientSessionIntSpec;
	ommClientSessionIntSpec.setClientSessionHandle (handle);
	rfa::common::Handle* session_item_handle = omm_provider_->registerClient (event_queue_.get(), &ommClientSessionIntSpec, *this, nullptr /* closure */);
	if (nullptr == session_item_handle)
		return false;
	session_item_handles_.push_back (session_item_handle);
	cumulative_stats_[PROVIDER_PC_CLIENT_SESSION_ACCEPTED]++;
	return true;
}

/* 7.4.7.1.2 Handling Consumer Client Session Events: Client session connection
 *           has been lost.
 *
 * When the provider receives this event it should stop sending any data to that
 * client session. Then it should remove references to the client session handle
 * and its associated request tokens.
 */
void
usagi::provider_t::processOMMInactiveClientSessionEvent (
	const rfa::sessionLayer::OMMInactiveClientSessionEvent& session_event
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_INACTIVE_CLIENT_SESSION_RECEIVED]++;
}

/* 7.5.8.1 Handling Item Events (Login Response Events).
 */
void
usagi::provider_t::processOMMItemEvent (
	const rfa::sessionLayer::OMMItemEvent&	item_event
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_ITEM_EVENTS_RECEIVED]++;
	const rfa::common::Msg& msg = item_event.getMsg();

/* Verify event is a response event */
	if (rfa::message::RespMsgEnum != msg.getMsgType()) {
		cumulative_stats_[PROVIDER_PC_OMM_ITEM_EVENTS_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << msg;
		return;
	}

	processRespMsg (static_cast<const rfa::message::RespMsg&>(msg));
}

/* 7.4.7.2 Handling consumer solicited item events.
 */
void
usagi::provider_t::processOMMSolicitedItemEvent (
	const rfa::sessionLayer::OMMSolicitedItemEvent&	item_event
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_SOLICITED_ITEM_EVENTS_RECEIVED]++;
	const rfa::common::Msg& msg = item_event.getMsg();

	switch (msg.getMsgType()) {
	case rfa::message::ReqMsgEnum:
		processReqMsg (static_cast<const rfa::message::ReqMsg&>(msg), item_event.getRequestToken());
		break;
	default:
		cumulative_stats_[PROVIDER_PC_OMM_SOLICITED_ITEM_EVENTS_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << msg;
		break;
	}
}

void
usagi::provider_t::processReqMsg (
	const rfa::message::ReqMsg& request_msg,
	rfa::sessionLayer::RequestToken& request_token
	)
{
	cumulative_stats_[PROVIDER_PC_REQUEST_MSGS_RECEIVED]++;
	switch (request_msg.getMsgModelType()) {
	case rfa::rdm::MMT_LOGIN:
		processLoginRequest (request_msg, request_token);
		break;
	case rfa::rdm::MMT_DIRECTORY:
		processDirectoryRequest (request_msg);
		break;
	case rfa::rdm::MMT_DICTIONARY:
		processDictionaryRequest (request_msg);
		break;
	case rfa::rdm::MMT_MARKET_PRICE:
		processMarketPriceRequest (request_msg);
		break;
	default:
		cumulative_stats_[PROVIDER_PC_REQUEST_MSGS_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << request_msg;
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
usagi::provider_t::processLoginRequest (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_REQUEST_RECEIVED]++;
/* Pass through RFA validation and report exceptions */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
/* 4.2.8 Message Validation. */
		RFA_String warningText;
		validation_status = login_msg.validateMsg (&warningText);
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_VALIDATED]++;
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(WARNING) << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_MALFORMED]++;
		LOG(WARNING) << "MMT_LOGIN::InvalidUsageException: { "
				   << login_msg <<
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
		LOG(ERROR) << "MMT_LOGIN::InvalidUsageException: { "
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
usagi::provider_t::rejectLogin (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	VLOG(2) << "Rejecting MMT_LOGIN request.";

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
	RFA_String warningText;
	const uint8_t validation_status = response.validateMsg (&warningText);
	if (rfa::message::MsgValidationWarning == validation_status) {
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_MALFORMED]++;
		LOG(ERROR) << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} else {
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_VALIDATED]++;
		DCHECK (rfa::message::MsgValidationOk == validation_status);
	}

	submit (static_cast<rfa::common::Msg&> (response), login_token, nullptr);
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_REQUEST_REJECTED]++;
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
usagi::provider_t::acceptLogin (
	const rfa::message::ReqMsg& login_msg,
	rfa::sessionLayer::RequestToken& login_token
	)
{
	VLOG(2) << "Accepting MMT_LOGIN request.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;
/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_LOGIN);
/* 7.5.9.3 Set response type.  RDM 3.2.2 RespMsg: Refresh when accepting login. */
	response.setRespType (rfa::message::RespMsg::RefreshEnum);
	response.setIndicationMask (rfa::message::RespMsg::RefreshCompleteFlag);

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
	RFA_String warningText;
	const uint8_t validation_status = response.validateMsg (&warningText);
	if (rfa::message::MsgValidationWarning == validation_status) {
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_MALFORMED]++;
		LOG(ERROR) << "MMT_LOGIN::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} else {
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_VALIDATED]++;
		DCHECK (rfa::message::MsgValidationOk == validation_status);
	}

	submit (static_cast<rfa::common::Msg&> (response), login_token, nullptr);
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_REQUEST_ACCEPTED]++;
	return true;
}

void
usagi::provider_t::processDirectoryRequest (
	const rfa::message::ReqMsg&	request_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_DIRECTORY_REQUEST_RECEIVED]++;
}

void
usagi::provider_t::processDictionaryRequest (
	const rfa::message::ReqMsg&	request_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_DICTIONARY_REQUEST_RECEIVED]++;
}

void
usagi::provider_t::processMarketPriceRequest (
	const rfa::message::ReqMsg&	request_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_MARKET_PRICE_REQUEST_RECEIVED]++;
}

void
usagi::provider_t::processRespMsg (
	const rfa::message::RespMsg&	reply_msg
	)
{
	cumulative_stats_[PROVIDER_PC_RESPONSE_MSGS_RECEIVED]++;
/* Verify event is a login response event */
	if (rfa::rdm::MMT_LOGIN != reply_msg.getMsgModelType()) {
		cumulative_stats_[PROVIDER_PC_RESPONSE_MSGS_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << reply_msg;
		return;
	}

	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_RECEIVED]++;
	const rfa::common::RespStatus& respStatus = reply_msg.getRespStatus();

/* save state */
	stream_state_ = respStatus.getStreamState();
	data_state_   = respStatus.getDataState();

	switch (stream_state_) {
	case rfa::common::RespStatus::OpenEnum:
		switch (data_state_) {
		case rfa::common::RespStatus::OkEnum:
			processLoginSuccess (reply_msg);
			break;

		case rfa::common::RespStatus::SuspectEnum:
			processLoginSuspect (reply_msg);
			break;

		default:
			cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_DISCARDED]++;
			LOG(WARNING) << "Uncaught: " << reply_msg;
			break;
		}
		break;

	case rfa::common::RespStatus::ClosedEnum:
		processLoginClosed (reply_msg);
		break;

	default:
		cumulative_stats_[PROVIDER_PC_MMT_LOGIN_RESPONSE_DISCARDED]++;
		LOG(WARNING) << "Uncaught: " << reply_msg;
		break;
	}
}

/* 7.5.8.1.1 Login Success.
 * The stream state is OpenEnum one has received login permission from the
 * back-end infrastructure and the non-interactive provider can start to
 * publish data, including the service directory, dictionary, and other
 * response messages of different message model types.
 */
void
usagi::provider_t::processLoginSuccess (
	const rfa::message::RespMsg&			login_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_SUCCESS_RECEIVED]++;
	try {
		sendDirectoryResponse();
		resetTokens();
		LOG(INFO) << "Unmuting provider.";
		is_muted_ = false;

/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "MMT_DIRECTORY::InvalidUsageException: { "
				"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
/* cannot publish until directory is sent. */
		return;
	}
}

/* 7.5.9 Sending Response Messages Using an OMM Non-Interactive Provider.
 * 10.4.3 Providing Service Directory.
 * Immediately after a successful login, and before publishing data, a non-
 * interactive provider must publish a service directory that indicates
 * services and capabilities associated with the non-interactive provider and
 * includes information about supported domain types, the service’s state, QoS,
 * and any item group information associated with the service.
 */
bool
usagi::provider_t::sendDirectoryResponse()
{
	VLOG(2) << "Sending directory response.";

/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response;

/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_DIRECTORY);
/* 7.5.9.3 Set response type. */
	response.setRespType (rfa::message::RespMsg::RefreshEnum);
/* 7.5.9.4 Set the response type enumation.
 * Note type is unsolicited despite being a mandatory requirement before
 * publishing.
 */
	response.setRespTypeNum (rfa::rdm::REFRESH_UNSOLICITED);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	rfa::message::AttribInfo attribInfo;

/* DataMask: required for refresh RespMsg
 *   SERVICE_INFO_FILTER  - Static information about service.
 *   SERVICE_STATE_FILTER - Refresh or update state.
 *   SERVICE_GROUP_FILTER - Transient groups within service.
 *   SERVICE_LOAD_FILTER  - Statistics about concurrent stream support.
 *   SERVICE_DATA_FILTER  - Broadcast data.
 *   SERVICE_LINK_FILTER  - Load balance grouping.
 */
	attribInfo.setDataMask (rfa::rdm::SERVICE_INFO_FILTER | rfa::rdm::SERVICE_STATE_FILTER);
/* Name:        Not used */
/* NameType:    Not used */
/* ServiceName: Not used */
/* ServiceId:   Not used */
/* Id:          Not used */
/* Attrib:      Not used */
	response.setAttribInfo (attribInfo);

/* 5.4.4 Versioning Support.  RFA Data and Msg interfaces provide versioning
 * functionality to allow application to encode data with a connection's
 * negotiated RWF version. Versioning support applies only to OMM connection
 * types and OMM message domain models.
 */
// not std::map :(  derived from rfa::common::Data
	rfa::data::Map map;
	getServiceDirectory (map);
	response.setPayload (map);

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
	RFA_String warningText;
	uint8_t validation_status = response.validateMsg (&warningText);
	if (rfa::message::MsgValidationWarning == validation_status) {
		cumulative_stats_[PROVIDER_PC_MMT_DIRECTORY_VALIDATED]++;
		LOG(ERROR) << "MMT_DIRECTORY::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} else {
		cumulative_stats_[PROVIDER_PC_MMT_DIRECTORY_MALFORMED]++;
		assert (rfa::message::MsgValidationOk == validation_status);
	}

/* Create and throw away first token for MMT_DIRECTORY. */
	submit (static_cast<rfa::common::Msg&> (response), omm_provider_->generateItemToken(), nullptr);
	cumulative_stats_[PROVIDER_PC_MMT_DIRECTORY_SENT]++;
	return true;
}

void
usagi::provider_t::getServiceDirectory (
	rfa::data::Map& map
	)
{
	rfa::data::MapWriteIterator it;
	rfa::data::MapEntry mapEntry;
	rfa::data::DataBuffer dataBuffer;
	rfa::data::FilterList filterList;
	const RFA_String serviceName (config_.service_name.c_str(), 0, false);

	map.setAssociatedMetaInfo (rwf_major_version_, rwf_minor_version_);
	it.start (map);

/* No idea ... */
	map.setKeyDataType (rfa::data::DataBuffer::StringAsciiEnum);
/* One service. */
	map.setTotalCountHint (1);

/* Service name -> service filter list */
	mapEntry.setAction (rfa::data::MapEntry::Add);
	dataBuffer.setFromString (serviceName, rfa::data::DataBuffer::StringAsciiEnum);
	mapEntry.setKeyData (dataBuffer);
	getServiceFilterList (filterList);
	mapEntry.setData (static_cast<rfa::common::Data&>(filterList));
	it.bind (mapEntry);

	it.complete();
}

void
usagi::provider_t::getServiceFilterList (
	rfa::data::FilterList& filterList
	)
{
	rfa::data::FilterListWriteIterator it;
	rfa::data::FilterEntry filterEntry;
	rfa::data::ElementList elementList;

	filterList.setAssociatedMetaInfo (rwf_major_version_, rwf_minor_version_);
	it.start (filterList);  

/* SERVICE_INFO_ID and SERVICE_STATE_ID */
	filterList.setTotalCountHint (2);

/* SERVICE_INFO_ID */
	filterEntry.setFilterId (rfa::rdm::SERVICE_INFO_ID);
	filterEntry.setAction (rfa::data::FilterEntry::Set);
	getServiceInformation (elementList);
	filterEntry.setData (static_cast<const rfa::common::Data&>(elementList));
	it.bind (filterEntry);

/* SERVICE_STATE_ID */
	filterEntry.setFilterId (rfa::rdm::SERVICE_STATE_ID);
	filterEntry.setAction (rfa::data::FilterEntry::Set);
	getServiceState (elementList);
	filterEntry.setData (static_cast<const rfa::common::Data&>(elementList));
	it.bind (filterEntry);

	it.complete();
}

/* SERVICE_INFO_ID
 * Information about a service that does not update very often.
 */
void
usagi::provider_t::getServiceInformation (
	rfa::data::ElementList& elementList
	)
{
	rfa::data::ElementListWriteIterator it;
	rfa::data::ElementEntry element;
	rfa::data::DataBuffer dataBuffer;
	rfa::data::Array array_;
	const RFA_String serviceName (config_.service_name.c_str(), 0, false);

	elementList.setAssociatedMetaInfo (rwf_major_version_, rwf_minor_version_);
	it.start (elementList);

/* Name<AsciiString>
 * Service name. This will match the concrete service name or the service group
 * name that is in the Map.Key.
 */
	element.setName (rfa::rdm::ENAME_NAME);
	dataBuffer.setFromString (serviceName, rfa::data::DataBuffer::StringAsciiEnum);
	element.setData (dataBuffer);
	it.bind (element);
	
/* Capabilities<Array of UInt>
 * Array of valid MessageModelTypes that the service can provide. The UInt
 * MesageModelType is extensible, using values defined in the RDM Usage Guide
 * (1-255). Login and Service Directory are omitted from this list. This
 * element must be set correctly because RFA will only request an item from a
 * service if the MessageModelType of the request is listed in this element.
 */
	element.setName (rfa::rdm::ENAME_CAPABILITIES);
	getServiceCapabilities (array_);
	element.setData (static_cast<const rfa::common::Data&>(array_));
	it.bind (element);

/* DictionariesUsed<Array of AsciiString>
 * List of Dictionary names that may be required to process all of the data 
 * from this service. Whether or not the dictionary is required depends on 
 * the needs of the consumer (e.g. display application, caching application)
 */
	element.setName (rfa::rdm::ENAME_DICTIONARYS_USED);
	getServiceDictionaries (array_);
	element.setData (static_cast<const rfa::common::Data&>(array_));
	it.bind (element);

	it.complete();
}

/* Array of valid MessageModelTypes that the service can provide.
 * rfa::data::Array does not require version tagging according to examples.
 */
void
usagi::provider_t::getServiceCapabilities (
	rfa::data::Array& capabilities
	)
{
	rfa::data::ArrayWriteIterator it;
	rfa::data::ArrayEntry arrayEntry;
	rfa::data::DataBuffer dataBuffer;

	it.start (capabilities);

/* MarketPrice = 6 */
	dataBuffer.setUInt32 (rfa::rdm::MMT_MARKET_PRICE);
	arrayEntry.setData (dataBuffer);
	it.bind (arrayEntry);

	it.complete();
}

void
usagi::provider_t::getServiceDictionaries (
	rfa::data::Array& dictionaries
	)
{
	rfa::data::ArrayWriteIterator it;
	rfa::data::ArrayEntry arrayEntry;
	rfa::data::DataBuffer dataBuffer;

	it.start (dictionaries);

/* RDM Field Dictionary */
	dataBuffer.setFromString (kRdmFieldDictionaryName, rfa::data::DataBuffer::StringAsciiEnum);
	arrayEntry.setData (dataBuffer);
	it.bind (arrayEntry);

/* Enumerated Type Dictionary */
	dataBuffer.setFromString (kEnumTypeDictionaryName, rfa::data::DataBuffer::StringAsciiEnum);
	arrayEntry.setData (dataBuffer);
	it.bind (arrayEntry);

	it.complete();
}

/* SERVICE_STATE_ID
 * State of a service.
 */
void
usagi::provider_t::getServiceState (
	rfa::data::ElementList& elementList
	)
{
	rfa::data::ElementListWriteIterator it;
	rfa::data::ElementEntry element;
	rfa::data::DataBuffer dataBuffer;

	elementList.setAssociatedMetaInfo (rwf_major_version_, rwf_minor_version_);
	it.start (elementList);

/* ServiceState<UInt>
 * 1: Up/Yes
 * 0: Down/No
 * Is the original provider of the data responding to new requests. All
 * existing streams are left unchanged.
 */
	element.setName (rfa::rdm::ENAME_SVC_STATE);
	dataBuffer.setUInt32 (1);
	element.setData (dataBuffer);
	it.bind (element);

/* AcceptingRequests<UInt>
 * 1: Yes
 * 0: No
 * If the value is 0, then consuming applications should not send any new
 * requests to the service provider. (Reissues may still be sent.) If an RFA
 * application makes new requests to the service, they will be queued. All
 * existing streams are left unchanged.
 */
#if 0
	element.setName (rfa::rdm::ENAME_ACCEPTING_REQS);
	dataBuffer.setUInt32 (1);
	element.setData (dataBuffer);
	it.bind (element);
#endif

	it.complete();
}

/* Iterate through entire item dictionary and re-generate tokens.
 */
bool
usagi::provider_t::resetTokens()
{
	if (!(bool)omm_provider_) {
		LOG(WARNING) << "Reset tokens whilst provider is invalid.";
		return false;
	}

	LOG(INFO) << "Resetting " << directory_.size() << " provider tokens.";
/* Cannot use std::for_each (auto λ) due to language limitations. */
	std::for_each (directory_.begin(), directory_.end(),
		[&](std::pair<std::string, std::weak_ptr<item_stream_t>> it)
	{
		if (auto sp = it.second.lock()) {
			sp->token = &( omm_provider_->generateItemToken() );
			assert (nullptr != sp->token);
			cumulative_stats_[PROVIDER_PC_TOKENS_GENERATED]++;
		}
	});
	return true;
}

/* 7.5.8.1.2 Other Login States.
 * All connections are down. The application should stop publishing; it may
 * resume once the data state becomes OkEnum.
 */
void
usagi::provider_t::processLoginSuspect (
	const rfa::message::RespMsg&			suspect_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_SUSPECT_RECEIVED]++;
	is_muted_ = true;
}

/* 7.5.8.1.2 Other Login States.
 * The login failed, and the provider application failed to get permission
 * from the back-end infrastructure. In this case, the provider application
 * cannot start to publish data.
 */
void
usagi::provider_t::processLoginClosed (
	const rfa::message::RespMsg&			logout_msg
	)
{
	cumulative_stats_[PROVIDER_PC_MMT_LOGIN_CLOSED_RECEIVED]++;
	is_muted_ = true;
}

/* 7.5.8.2 Handling CmdError Events.
 * Represents an error Event that is generated during the submit() call on the
 * OMM non-interactive provider. This Event gives the provider application
 * access to the Cmd, CmdID, closure and OMMErrorStatus for the Cmd that
 * failed.
 */
void
usagi::provider_t::processOMMCmdErrorEvent (
	const rfa::sessionLayer::OMMCmdErrorEvent& error
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_CMD_ERRORS]++;
	LOG(ERROR) << "OMMCmdErrorEvent: { "
		  "\"CmdId\": " << error.getCmdID() <<
		", \"State\": " << error.getStatus().getState() <<
		", \"StatusCode\": " << error.getStatus().getStatusCode() <<
		", \"StatusText\": \"" << error.getStatus().getStatusText() << "\""
		" }";
}

/* eof */
