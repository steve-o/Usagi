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
#include "client.hh"

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
	std::shared_ptr<rfa::common::EventQueue> event_queue,
	std::shared_ptr<void> zmq_context
	) :
	creation_time_ (boost::posix_time::second_clock::universal_time()),
	last_activity_ (creation_time_),
	config_ (config),
	rfa_ (rfa),
	event_queue_ (event_queue),
	error_item_handle_ (nullptr),
	min_rwf_version_ (0),
	service_id_ (0),
	response_ (false),	/* reference */
	array_ (false),		/* reference */
	elementList_ (false),	/* reference */
	map_ (false),		/* reference */
	attribInfo_ (false),	/* reference */
	is_accepting_connections_ (true),
	is_accepting_requests_ (true),
	zmq_context_ (zmq_context)
{
	ZeroMemory (cumulative_stats_, sizeof (cumulative_stats_));
	ZeroMemory (snap_stats_, sizeof (snap_stats_));
}

usagi::provider_t::~provider_t()
{
/* clients */
	VLOG(3) << "Unregistering " << clients_.size() << " RFA session clients.";
	std::for_each (clients_.begin(), clients_.end(),
		[&](std::pair<rfa::common::Handle*const, std::shared_ptr<client_t>>& client)
	{
		CHECK ((bool)client.second);
		omm_provider_->unregisterClient (client.first);
		client.second.reset();
	});
/* 0mq context */
	CHECK (request_sock_.use_count() <= 1);
	request_sock_.reset();
	CHECK (response_sock_.use_count() <= 1);
	response_sock_.reset();
	zmq_context_.reset();
/* RFA handles */
	if (nullptr != error_item_handle_)
		omm_provider_->unregisterClient (error_item_handle_), error_item_handle_ = nullptr;
	omm_provider_.reset();
	session_.reset();
	event_queue_.reset();
	rfa_.reset();
/* Summary output */
	using namespace boost::posix_time;
	auto uptime = second_clock::universal_time() - creation_time_;
	VLOG(3) << "Provider summary: {"
		 " \"Uptime\": \"" << to_simple_string (uptime) << "\""
		", \"MsgsSent\": " << cumulative_stats_[PROVIDER_PC_MSGS_SENT] <<
		", \"RfaEventsReceived\": " << cumulative_stats_[PROVIDER_PC_RFA_EVENTS_RECEIVED] <<
		", \"OmmCommandErrors\": " << cumulative_stats_[PROVIDER_PC_OMM_CMD_ERRORS] <<
		", \"ConnectionEvents\": " << cumulative_stats_[PROVIDER_PC_CONNECTION_EVENTS_RECEIVED] <<
		", \"ClientSessions\": " << cumulative_stats_[PROVIDER_PC_CLIENT_SESSION_ACCEPTED] <<
		" }";
}

bool
usagi::provider_t::Init()
{
	last_activity_ = boost::posix_time::second_clock::universal_time();

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

/* Pre-allocate memory buffer for payload iterator */
	CHECK (config_.maximum_data_size > 0);
	map_it_.initialize (map_, (uint32_t)config_.maximum_data_size);
	CHECK (map_it_.isInitialized());
	element_it_.initialize (elementList_, (uint32_t)config_.maximum_data_size);
	CHECK (element_it_.isInitialized());

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
	connection_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommListenerConnectionIntSpec, *static_cast<rfa::common::Client*> (this), nullptr /* closure */);
	if (nullptr == connection_item_handle_)
		return false;

/* listen events. */
	VLOG(3) << "Registering listen interest.";
	rfa::sessionLayer::OMMClientSessionListenerIntSpec ommClientSessionListenerIntSpec;
	listen_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommClientSessionListenerIntSpec, *static_cast<rfa::common::Client*> (this), nullptr /* closure */);
	if (nullptr == listen_item_handle_)
		return false;

/* receive error events (OMMCmdErrorEvent) related to calls to submit(). */
	VLOG(3) << "Registering OMM error interest.";
	rfa::sessionLayer::OMMErrorIntSpec ommErrorIntSpec;
	error_item_handle_ = omm_provider_->registerClient (event_queue_.get(), &ommErrorIntSpec, *static_cast<rfa::common::Client*> (this), nullptr /* closure */);
	if (nullptr == error_item_handle_)
		return false;

/* create new push socket for submitting refresh requests. */
	try {
		std::function<int(void*)> zmq_close_deleter = zmq_close;
		request_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)request_sock_);
		int rc = zmq_bind (request_sock_.get(), "inproc://usagi/rfa/request");
		CHECK(0 == rc);
/* socket for fanning out broadcast images. */
		response_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)response_sock_);
		rc = zmq_connect (response_sock_.get(), "inproc://usagi/rfa/response");
		CHECK(0 == rc);
	} catch (std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		return false;
	}
	return true;
}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
bool
usagi::provider_t::CreateItemStream (
	const char* name,
	std::shared_ptr<item_stream_t> item_stream
	)
{
	item_stream->rfa_name.set (name, 0, true);
/* no tokens until subscription appears. */
	const std::string key (name);
	boost::unique_lock<boost::shared_mutex> lock (directory_lock_);
	auto status = directory_.emplace (std::make_pair (key, item_stream));
	VLOG(4) << "Creating item stream #" << directory_.size() << " for RIC \"" << name << "\".";
	assert (true == status.second);
	assert (directory_.end() != directory_.find (key));
	last_activity_ = boost::posix_time::second_clock::universal_time();
	return true;
}

/* Send an Rfa message through the pre-created item stream.
 */

bool
usagi::provider_t::Send (
	item_stream_t& stream,
	rfa::message::RespMsg& response_msg,
	const rfa::message::AttribInfo& attribInfo
)
{
	unsigned i = 0;
	boost::shared_lock<boost::shared_mutex> stream_lock (stream.lock);
	const auto now = boost::posix_time::second_clock::universal_time();
	const rfa::common::Buffer& buffer = response_msg.getEncodedBuffer();
	provider::Response response;
	zmq_msg_t msg;

	response.set_msg_type (provider::Response::MSG_UPDATE);
/* first iteration without AttribInfo */
	std::for_each (stream.requests.begin(), stream.requests.end(),
		[&](std::pair<rfa::sessionLayer::RequestToken*const, std::shared_ptr<request_t>>& request)
	{
		if (!request.second->has_initial_image.load() || request.second->use_attribinfo_in_updates)
			return;
/* pack into 0mq message. */
		response.set_token (reinterpret_cast<uintptr_t> (request.first));
		response.set_encoded_buffer (buffer.c_buf(), buffer.size());
		int rc = zmq_msg_init_size (&msg, response.ByteSize());
		CHECK(0 == rc);
		response.SerializeToArray (zmq_msg_data (&msg), static_cast<int> (zmq_msg_size (&msg)));
		rc = zmq_send (response_sock_.get(), &msg, 0);
		CHECK(0 == rc);
		rc = zmq_msg_close (&msg);
		CHECK(0 == rc);
/* update client send statistics. */
		auto client = request.second->client.lock();
		if ((bool)client) {
			client->cumulative_stats_[CLIENT_PC_RFA_MSGS_SENT]++;
			client->last_activity_ = now;
		}
		++i;
	});
/* second iteration with AttribInfo */
	if (i < stream.requests.size())
	{
		response_msg.setAttribInfo (attribInfo);
/* re-encode */
		const rfa::common::Buffer& buffer = response_msg.getEncodedBuffer();
		std::for_each (stream.requests.begin(), stream.requests.end(),
		[&](std::pair<rfa::sessionLayer::RequestToken*const, std::shared_ptr<request_t>>& request)
		{
			if (!request.second->has_initial_image.load() || !request.second->use_attribinfo_in_updates)
				return;
/* pack into 0mq message. */
			response.set_token (reinterpret_cast<uintptr_t> (request.first));
			response.set_encoded_buffer (buffer.c_buf(), buffer.size());
			int rc = zmq_msg_init_size (&msg, response.ByteSize());
			CHECK(0 == rc);
			response.SerializeToArray (zmq_msg_data (&msg), static_cast<int> (zmq_msg_size (&msg)));
			rc = zmq_send (response_sock_.get(), &msg, 0);
			CHECK(0 == rc);
			rc = zmq_msg_close (&msg);
			CHECK(0 == rc);
/* update client send statistics. */
			auto client = request.second->client.lock();
			if ((bool)client) {
				client->cumulative_stats_[CLIENT_PC_RFA_MSGS_SENT]++;
				client->last_activity_ = now;
			}
			++i;
		});
	}
	cumulative_stats_[PROVIDER_PC_MSGS_SENT]++;
	last_activity_ = now;
	return true;
}

/* Send an Rfa initial image to a single client.
 */
bool
usagi::provider_t::SendReply (
	rfa::message::RespMsg& msg,
	rfa::sessionLayer::RequestToken& token
	)
{
/* find request and iteam stream for the token */
	boost::shared_lock<boost::shared_mutex> requests_lock (requests_lock_);
	auto it = requests_.find (&token);
	if (requests_.end() == it)
		return false;
	auto request = it->second.lock();	// weak_ptr for request_t
	if (!(bool)request)
		return false;
	auto stream = request->item_stream.lock();	// weak_ptr for item_stream_t
	if (!(bool)stream)
		return false;
	auto client = request->client.lock();	// weak_ptr for client_t;
	if (!(bool)client)
		return false;
/* lock updates for this stream */
	boost::unique_lock<boost::shared_mutex> stream_lock (stream->lock);
/* forward refresh image */
	Submit (static_cast<rfa::common::Msg&> (msg), token, nullptr);
	cumulative_stats_[PROVIDER_PC_MSGS_SENT]++;
	client->cumulative_stats_[CLIENT_PC_RFA_MSGS_SENT]++;
	client->last_activity_ = last_activity_ = boost::posix_time::second_clock::universal_time();
/* unlock */
	return true;
}

/* 7.4.8 Sending response messages using an OMM provider.
 */
uint32_t
usagi::provider_t::Submit (
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
		OnConnectionEvent(static_cast<const rfa::sessionLayer::ConnectionEvent&>(event_));
		break;

	case rfa::sessionLayer::OMMActiveClientSessionEventEnum:
		OnOMMActiveClientSessionEvent(static_cast<const rfa::sessionLayer::OMMActiveClientSessionEvent&>(event_));
		break;

        case rfa::sessionLayer::OMMCmdErrorEventEnum:
                OnOMMCmdErrorEvent (static_cast<const rfa::sessionLayer::OMMCmdErrorEvent&>(event_));
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
usagi::provider_t::OnConnectionEvent (
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
usagi::provider_t::OnOMMActiveClientSessionEvent (
	const rfa::sessionLayer::OMMActiveClientSessionEvent& session_event
	)
{
	cumulative_stats_[PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_RECEIVED]++;
	try {
		if (!is_accepting_connections_ || clients_.size() == config_.session_capacity)
			RejectClientSession (session_event.getClientSessionHandle());
		else
			AcceptClientSession (session_event.getClientSessionHandle());
/* ignore any error */
	} catch (rfa::common::InvalidUsageException& e) {
		cumulative_stats_[PROVIDER_PC_OMM_ACTIVE_CLIENT_SESSION_EXCEPTION]++;
		LOG(ERROR) << "OMMActiveClientSession::InvalidUsageException: { "
				"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
}

bool
usagi::provider_t::RejectClientSession (
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
usagi::provider_t::AcceptClientSession (
	const rfa::common::Handle* handle
	)
{
	VLOG(2) << "Accepting new client session request.";

	auto client = std::make_shared<client_t> (*this, handle);
	if (!(bool)client)
		return false;

/* 7.4.7.2.1 Handling login requests. */
	rfa::sessionLayer::OMMClientSessionIntSpec ommClientSessionIntSpec;
	ommClientSessionIntSpec.setClientSessionHandle (handle);
	auto registered_handle = omm_provider_->registerClient (event_queue_.get(), &ommClientSessionIntSpec, *static_cast<rfa::common::Client*> (client.get()), nullptr /* closure */);
	if (nullptr == registered_handle)
		return false;
	if (!client->Init (registered_handle))
		return false;
	if (!client->GetAssociatedMetaInfo()) {
		omm_provider_->unregisterClient (registered_handle);
		return false;
	}

/* Determine lowest common Reuters Wire Format (RWF) version */
	const uint16_t client_rwf_version = (client->GetRwfMajorVersion() * 256) + client->GetRwfMinorVersion();
	if (0 == min_rwf_version_)
	{
		LOG(INFO) << "Setting RWF: { "
				  "\"MajorVersion\": " << (unsigned)client->GetRwfMajorVersion() <<
				", \"MinorVersion\": " << (unsigned)client->GetRwfMinorVersion() <<
				" }";
		min_rwf_version_.store (client_rwf_version);
	}
	else if (min_rwf_version_ > client_rwf_version)
	{
		LOG(INFO) << "Degrading RWF: { "
				  "\"MajorVersion\": " << (unsigned)client->GetRwfMajorVersion() <<
				", \"MinorVersion\": " << (unsigned)client->GetRwfMinorVersion() <<
				" }";
		min_rwf_version_.store (client_rwf_version);
	}

	boost::unique_lock<boost::shared_mutex> lock (clients_lock_);
	clients_.emplace (std::make_pair (registered_handle, client));
	cumulative_stats_[PROVIDER_PC_CLIENT_SESSION_ACCEPTED]++;
	return true;
}

bool
usagi::provider_t::EraseClientSession (
	rfa::common::Handle* handle
	)
{
/* unregister RFA client session. */
	omm_provider_->unregisterClient (handle);
/* remove client from directory. */
	boost::upgrade_lock<boost::shared_mutex> lock (clients_lock_);
	auto it = clients_.find (handle);
	if (clients_.end() == it)
		return false;
	boost::upgrade_to_unique_lock<boost::shared_mutex> uniqueLock (lock);
	clients_.erase (it);
	return true;
}

/* 7.3.5.5 Making Request for Service Directory
 * By default, information about all available services is returned. If an
 * application wishes to make a request for information pertaining to a 
 * specific service only, it can use the setServiceName() method of the request
 * AttribInfo.
 *
 * The setDataMask() method accepts a bit mask that determines what information
 * is returned for each service. The bit mask values for the Service Filter are
 * defined in Include/RDM/RDM.h. The data associated with each specified bit 
 * mask value is returned in a separate ElementList contained in a FilterEntry.
 * The ServiceInfo ElementList contains the name and capabilities of the 
 * source. The ServiceState ElementList contains information related to the 
 * availability of the service.
 */
void
usagi::provider_t::GetDirectoryResponse (
	rfa::message::RespMsg* response,
	uint8_t rwf_major_version,
	uint8_t rwf_minor_version,
	const char* service_name,
	uint32_t filter_mask,
	uint8_t response_type
	)
{
	CHECK (nullptr != response);
	CHECK (response_type == rfa::rdm::REFRESH_UNSOLICITED || response_type == rfa::rdm::REFRESH_SOLICITED);
/* 7.5.9.2 Set the message model type of the response. */
	response->setMsgModelType (rfa::rdm::MMT_DIRECTORY);
/* 7.5.9.3 Set response type. */
	response->setRespType (rfa::message::RespMsg::RefreshEnum);
/* 7.5.9.4 Set the response type enumation.
 * Note type is unsolicited despite being a mandatory requirement before
 * publishing.
 */
	response->setRespTypeNum (response_type);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	attribInfo_.clear();

/* DataMask: required for refresh RespMsg
 *   SERVICE_INFO_FILTER  - Static information about service.
 *   SERVICE_STATE_FILTER - Refresh or update state.
 *   SERVICE_GROUP_FILTER - Transient groups within service.
 *   SERVICE_LOAD_FILTER  - Statistics about concurrent stream support.
 *   SERVICE_DATA_FILTER  - Broadcast data.
 *   SERVICE_LINK_FILTER  - Load balance grouping.
 */
	attribInfo_.setDataMask (filter_mask & (rfa::rdm::SERVICE_INFO_FILTER | rfa::rdm::SERVICE_STATE_FILTER));
/* Name:        Not used */
/* NameType:    Not used */
/* ServiceName: Not used */
/* ServiceId:   Not used */
/* Id:          Not used */
/* Attrib:      Not used */
	response->setAttribInfo (attribInfo_);

/* 5.4.4 Versioning Support.  RFA Data and Msg interfaces provide versioning
 * functionality to allow application to encode data with a connection's
 * negotiated RWF version. Versioning support applies only to OMM connection
 * types and OMM message domain models.
 */
// not std::map :(  derived from rfa::common::Data
	map_.clear();
	DCHECK (map_it_.isInitialized());
	map_it_.clear();
	GetServiceDirectory (&map_, &map_it_, rwf_major_version, rwf_minor_version, service_name, filter_mask);
	response->setPayload (map_);

	status_.clear();
/* Item interaction state: Open, Closed, ClosedRecover, Redirected, NonStreaming, or Unspecified. */
	status_.setStreamState (rfa::common::RespStatus::OpenEnum);
/* Data quality state: Ok, Suspect, or Unspecified. */
	status_.setDataState (rfa::common::RespStatus::OkEnum);
/* Error code, e.g. NotFound, InvalidArgument, ... */
	status_.setStatusCode (rfa::common::RespStatus::NoneEnum);
	response->setRespStatus (status_);
}

/* Populate map with service directory, must be cleared before call.
 */
void
usagi::provider_t::GetServiceDirectory (
	rfa::data::Map* map,
	rfa::data::SingleWriteIterator* it,
	uint8_t rwf_major_version,
	uint8_t rwf_minor_version,
	const char* service_name,
	uint32_t filter_mask
	)
{
/* Recommended meta-data for a map. */
	map->setAssociatedMetaInfo (rwf_major_version, rwf_minor_version);
/* No idea ... */
	map->setKeyDataType (rfa::data::DataBuffer::StringAsciiEnum);

/* Request service filter does not match provided service. */
	const RFA_String serviceName (config_.service_name.c_str(), 0, false);
	if (nullptr != service_name && 0 != serviceName.compareCase (service_name))
	{
		return;
	}

/* One service. */
	map->setTotalCountHint (1);
	map->setIndicationMask (rfa::data::Map::EntriesFlag);

/* Clear required for SingleWriteIterator state machine. */
	DCHECK (it->isInitialized());
	it->start (map_, rfa::data::FilterListEnum);

	rfa::data::MapEntry mapEntry (false);
	rfa::data::DataBuffer dataBuffer (false);
/* Service name -> service filter list */
	mapEntry.setAction (rfa::data::MapEntry::Add);
/* iterator does not support direct writing for MapEntry value. */
	dataBuffer.setFromString (serviceName, rfa::data::DataBuffer::StringAsciiEnum);
	mapEntry.setKeyData (dataBuffer);
	it->bind (mapEntry);

	GetServiceFilterList (it, rwf_major_version, rwf_minor_version, filter_mask);

	it->complete();
}

void
usagi::provider_t::GetServiceFilterList (
	rfa::data::SingleWriteIterator* it,
	uint8_t rwf_major_version,
	uint8_t rwf_minor_version,
	uint32_t filter_mask
	)
{
/* Determine entry count for encoder hinting */
	const bool use_info_filter  = (0 != (filter_mask & rfa::rdm::SERVICE_INFO_FILTER));
	const bool use_state_filter = (0 != (filter_mask & rfa::rdm::SERVICE_STATE_FILTER));
	const unsigned filter_count = (use_info_filter ? 1 : 0) + (use_state_filter ? 1 : 0);
	
/* 5.3.8 Encoding with a SingleWriteIterator
 * Re-use of SingleWriteIterator permitted cross MapEntry and FieldList.
 */
	filterList_.setAssociatedMetaInfo (rwf_major_version, rwf_minor_version);

	DCHECK (it->isInitialized());
	it->start (filterList_, rfa::data::ElementListEnum);  

	rfa::data::FilterEntry filterEntry (false);
	filterEntry.setAction (rfa::data::FilterEntry::Set);
	filterList_.setTotalCountHint (filter_count);

	if (use_info_filter) {
		filterEntry.setFilterId (rfa::rdm::SERVICE_INFO_ID);
		it->bind (filterEntry, rfa::data::ElementListEnum);
		GetServiceInformation (it, rwf_major_version, rwf_minor_version);
	}
	if (use_state_filter) {
		filterEntry.setFilterId (rfa::rdm::SERVICE_STATE_ID);
		it->bind (filterEntry, rfa::data::ElementListEnum);
		GetServiceState (it, rwf_major_version, rwf_minor_version);
	}

	it->complete();
}

/* SERVICE_INFO_ID
 * Information about a service that does not update very often.
 */
void
usagi::provider_t::GetServiceInformation (
	rfa::data::SingleWriteIterator* it,
	uint8_t rwf_major_version,
	uint8_t rwf_minor_version
	)
{
	elementList_.setAssociatedMetaInfo (rwf_major_version, rwf_minor_version);

	DCHECK (it->isInitialized());
	it->start (elementList_);

	rfa::data::ElementEntry element (false);
/* Name<AsciiString>
 * Service name. This will match the concrete service name or the service group
 * name that is in the Map.Key.
 */
	element.setName (rfa::rdm::ENAME_NAME);
	it->bind (element);
	const RFA_String serviceName (config_.service_name.c_str(), 0, false);
	it->setString (serviceName, rfa::data::DataBuffer::StringAsciiEnum);
	
/* Capabilities<Array of UInt>
 * Array of valid MessageModelTypes that the service can provide. The UInt
 * MesageModelType is extensible, using values defined in the RDM Usage Guide
 * (1-255). Login and Service Directory are omitted from this list. This
 * element must be set correctly because RFA will only request an item from a
 * service if the MessageModelType of the request is listed in this element.
 */
	element.setName (rfa::rdm::ENAME_CAPABILITIES);
	it->bind (element);
	GetServiceCapabilities (it);

/* DictionariesUsed<Array of AsciiString>
 * List of Dictionary names that may be required to process all of the data 
 * from this service. Whether or not the dictionary is required depends on 
 * the needs of the consumer (e.g. display application, caching application)
 */
	element.setName (rfa::rdm::ENAME_DICTIONARYS_USED);
	it->bind (element);
	GetServiceDictionaries (it);

	it->complete();
}

/* Array of valid MessageModelTypes that the service can provide.
 * rfa::data::Array does not require version tagging according to examples.
 */
void
usagi::provider_t::GetServiceCapabilities (
	rfa::data::SingleWriteIterator* it
	)
{
	DCHECK (it->isInitialized());
	it->start (array_, rfa::data::DataBuffer::UIntEnum);

	rfa::data::ArrayEntry arrayEntry (false);
/* MarketPrice = 6 */
	it->bind (arrayEntry);
	it->setUInt (rfa::rdm::MMT_MARKET_PRICE);

	it->complete();
}

void
usagi::provider_t::GetServiceDictionaries (
	rfa::data::SingleWriteIterator* it
	)
{
	DCHECK (it->isInitialized());
	it->start (array_, rfa::data::DataBuffer::StringAsciiEnum);

	rfa::data::ArrayEntry arrayEntry (false);
/* RDM Field Dictionary */
	it->bind (arrayEntry);
	it->setString (kRdmFieldDictionaryName, rfa::data::DataBuffer::StringAsciiEnum);

/* Enumerated Type Dictionary */
	it->bind (arrayEntry);
	it->setString (kEnumTypeDictionaryName, rfa::data::DataBuffer::StringAsciiEnum);

	it->complete();
}

/* SERVICE_STATE_ID
 * State of a service.
 */
void
usagi::provider_t::GetServiceState (
	rfa::data::SingleWriteIterator* it,
	uint8_t rwf_major_version,
	uint8_t rwf_minor_version
	)
{
	elementList_.setAssociatedMetaInfo (rwf_major_version, rwf_minor_version);

	DCHECK (it->isInitialized());
	it->start (elementList_);

	rfa::data::ElementEntry element (false);
/* ServiceState<UInt>
 * 1: Up/Yes
 * 0: Down/No
 * Is the original provider of the data responding to new requests. All
 * existing streams are left unchanged.
 */
	element.setName (rfa::rdm::ENAME_SVC_STATE);
	it->bind (element);
	it->setUInt (1);

/* AcceptingRequests<UInt>
 * 1: Yes
 * 0: No
 * If the value is 0, then consuming applications should not send any new
 * requests to the service provider. (Reissues may still be sent.) If an RFA
 * application makes new requests to the service, they will be queued. All
 * existing streams are left unchanged.
 */
	element.setName (rfa::rdm::ENAME_ACCEPTING_REQS);
	it->bind (element);
	it->setUInt (is_accepting_requests_ ? 1 : 0);

	it->complete();
}

/* 7.5.8.2 Handling CmdError Events.
 * Represents an error Event that is generated during the submit() call on the
 * OMM non-interactive provider. This Event gives the provider application
 * access to the Cmd, CmdID, closure and OMMErrorStatus for the Cmd that
 * failed.
 */
void
usagi::provider_t::OnOMMCmdErrorEvent (
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