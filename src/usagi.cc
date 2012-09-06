/* Interactive RDM publisher application.
 */

#include "usagi.hh"

#define __STDC_FORMAT_MACROS
#include <cstdint>
#include <inttypes.h>

#include <windows.h>

/* ZeroMQ messaging middleware. */
#include <zmq.h>

#include "chromium/logging.hh"
#include "error.hh"
#include "rfa_logging.hh"
#include "rfaostream.hh"

#pragma warning( push )
#pragma warning( disable : 4244 4267 )
#include "provider.pb.h"
#pragma warning( pop )

/* RDM Usage Guide: Section 6.5: Enterprise Platform
 * For future compatibility, the DictionaryId should be set to 1 by providers.
 * The DictionaryId for the RDMFieldDictionary is 1.
 */
static const int kDictionaryId = 1;

/* RDM: Absolutely no idea. */
static const int kFieldListId = 3;

/* RDM Field Identifiers. */
static const int kRdmRdnDisplayId = 2;		/* RDNDISPLAY */
static const int kRdmTradePriceId = 6;		/* TRDPRC_1 */

using rfa::common::RFA_String;

static SOCKET g_abort_sock = INVALID_SOCKET;

class usagi::worker_t
{
public:
	worker_t (std::shared_ptr<provider_t> provider, const config_t& config, std::shared_ptr<void> zmq_context, unsigned id) :
		id_ (id),
		zmq_context_ (zmq_context),
		response_msg_ (false),	/* reference */
		fields_ (false),
		attribInfo_ (false),
		provider_ (provider),
		config_ (config)
	{
/* constants */

/* Set logger ID */
		std::ostringstream ss;
		ss << "Worker " << std::hex << std::setiosflags (std::ios_base::showbase) << id << ':';
		prefix_.assign (ss.str());
	}

	~worker_t()
	{
		CHECK (request_sock_.use_count() <= 1);
		request_sock_.reset();
		CHECK (response_sock_.use_count() <= 1);
		response_sock_.reset();
		zmq_context_.reset();
	}

	bool Init (void)
	{
		std::function<int(void*)> zmq_close_deleter = zmq_close;
		int rc;

		try {
/* Setup 0mq sockets */
			request_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PULL), zmq_close_deleter);
			CHECK((bool)request_sock_);
			rc = zmq_connect (request_sock_.get(), "inproc://usagi/rfa/request");
			CHECK(0 == rc);
/* Also bind for terminating interrupt. */
			rc = zmq_connect (request_sock_.get(), "inproc://usagi/worker/abort");
			CHECK(0 == rc);
/* Response image socket */
			response_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
			CHECK((bool)response_sock_);
			rc = zmq_connect (response_sock_.get(), "inproc://usagi/rfa/response");
			CHECK(0 == rc);
		} catch (std::exception& e) {
			LOG(ERROR) << prefix_ << "ZeroMQ::Exception: { "
				"\"What\": \"" << e.what() << "\""
				" }";
			return false;
		}

		try {
/* Pre-allocate memory buffer for RFA payload iterator */
			fields_ = std::make_shared<rfa::data::FieldList> ();
			CHECK ((bool)fields_);
			fields_->setInfo (kDictionaryId, kFieldListId);

			CHECK (config_.maximum_data_size > 0);
			single_write_it_ = std::make_shared<rfa::data::SingleWriteIterator> ();
			CHECK ((bool)single_write_it_);
			single_write_it_->initialize (*fields_.get(), static_cast<int> (config_.maximum_data_size));
			CHECK (single_write_it_->isInitialized());
		} catch (rfa::common::InvalidUsageException& e) {
			LOG(ERROR) << prefix_ << "InvalidUsageException: { "
				  "\"Severity\": \"" << severity_string (e.getSeverity()) << "\""
				", \"Classification\": \"" << classification_string (e.getClassification()) << "\""
				", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
			return false;
		}
		return true;
	}

	void Run (void)
	{
		provider::Response_MsgType msg_type;
		LOG(INFO) << prefix_ << "Accepting requests.";
		while (true)
		{
again:
			if (!GetRequest (&request_))
				continue;
			
			switch (request_.msg_type()) {
			case provider::Request::MSG_SUBSCRIPTION:
				msg_type = provider::Response::MSG_INITIAL;
				break;
			case provider::Request::MSG_REFRESH:
				msg_type = provider::Response::MSG_REFRESH;
				break;
			case provider::Request::MSG_ABORT:
				LOG(INFO) << prefix_ << "Received interrupt request.";
				goto close_worker;
			default:
				LOG(ERROR) << prefix_ << "Received unknown request.";
				goto again;
			}
			VLOG(1) << prefix_ << "Received request \"" << request_.refresh().item_name() << "\"";
			DVLOG(1) << prefix_ << request_.DebugString();

			try {
				OnRequest (reinterpret_cast<rfa::sessionLayer::RequestToken*> (static_cast<uintptr_t> (request_.refresh().token())),
					   request_.refresh().service_id(),
					   request_.refresh().model_type(),
					   request_.refresh().item_name().c_str(),
					   msg_type,
					   request_.refresh().rwf_major_version(),
					   request_.refresh().rwf_minor_version());
			} catch (std::exception& e) {
				LOG(ERROR) << prefix_ << "ProcessRequest::Exception: { "
					"\"What\": \"" << e.what() << "\""
					" }";
			}
		}

close_worker:
		LOG(INFO) << prefix_ << "Worker closed.";
	}

	unsigned GetId() const { return id_; }

protected:
	bool GetRequest (provider::Request*const request)
	{
		int rc;

		rc = zmq_msg_init (&msg_);
		CHECK(0 == rc);
		VLOG(1) << prefix_ << "Awaiting new job.";
		rc = zmq_recv (request_sock_.get(), &msg_, 0);
		CHECK(0 == rc);
		if (!request->ParseFromArray (zmq_msg_data (&msg_), static_cast<int> (zmq_msg_size (&msg_)))) {
			LOG(ERROR) << prefix_ << "Received invalid request.";
			rc = zmq_msg_close (&msg_);
			CHECK(0 == rc);
			return false;
		}
		rc = zmq_msg_close (&msg_);
		CHECK(0 == rc);
		return true;
	}

	void OnRequest (
		rfa::sessionLayer::RequestToken*const request_token,
		uint32_t service_id,
		uint8_t model_type,
		const char* item_name,
		provider::Response_MsgType msg_type,
		uint8_t rwf_major_version,
		uint8_t rwf_minor_version
		)
	{
		VLOG(2) << "Sending blank response to incoming refresh request: { "
			  "\"RequestToken\": \"" << request_token << "\""
			", \"ServiceID\": " << service_id <<
			", \"MsgModelType\": " << (int)model_type <<
			", \"Name\": \"" << item_name << "\""
			", \"RwfMajorVersion\": " << (int)rwf_major_version <<
			", \"RwfMinorVersion\": " << (int)rwf_minor_version <<
			" }";
/* 7.5.9.1 Create a response message (4.2.2) */
		response_msg_.clear();

/* 7.5.9.2 Set the message model type of the response. */
		response_msg_.setMsgModelType (model_type);
/* 7.5.9.3 Set response type. */
		response_msg_.setRespType (rfa::message::RespMsg::RefreshEnum);
		response_msg_.setIndicationMask (rfa::message::RespMsg::RefreshCompleteFlag | rfa::message::RespMsg::DoNotRippleFlag);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
		attribInfo_.clear();
		attribInfo_.setNameType (rfa::rdm::INSTRUMENT_NAME_RIC);
		RFA_String name (item_name, 0, false);	/* reference */
		attribInfo_.setServiceID (service_id);
		attribInfo_.setName (name);
		response_msg_.setAttribInfo (attribInfo_);

/* 4.3.1 RespMsg.Payload */
// not std::map :(  derived from rfa::common::Data
		fields_->setAssociatedMetaInfo (rwf_major_version, rwf_minor_version);

/* Clear required for SingleWriteIterator state machine. */
		auto& it = *single_write_it_.get();
		DCHECK (it.isInitialized());
		it.clear();
		it.start (*fields_.get());

		rfa::data::FieldEntry field (false);
		field.setFieldID (kRdmRdnDisplayId);
		it.bind (field);
		it.setUInt (100);

		it.complete();
/* Set a reference to field list, not a copy */
		response_msg_.setPayload (*fields_.get());

/** Optional: but require to replace stale values in cache when stale values are supported. **/
		status_.clear();
/* Item interaction state: Open, Closed, ClosedRecover, Redirected, NonStreaming, or Unspecified. */
		status_.setStreamState (rfa::common::RespStatus::OpenEnum);
/* Data quality state: Ok, Suspect, or Unspecified. */
		status_.setDataState (rfa::common::RespStatus::OkEnum);
/* Error code, e.g. NotFound, InvalidArgument, ... */
		status_.setStatusCode (rfa::common::RespStatus::NoneEnum);
		response_msg_.setRespStatus (status_);

#ifdef DEBUG
/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
		uint8_t validation_status = rfa::message::MsgValidationError;
		try {
			RFA_String warningText;
			validation_status = response_.validateMsg (&warningText);
			if (rfa::message::MsgValidationWarning == validation_status)
				LOG(ERROR) << prefix_ << "validateMsg: { \"warningText\": \"" << warningText << "\" }";
		} catch (rfa::common::InvalidUsageException& e) {
			LOG(ERROR) << prefix_ <<
				"InvalidUsageException: { " <<
				   "\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				", " << response_ <<
				" }";
		}
#endif
		const rfa::common::Buffer& buffer = response_msg_.getEncodedBuffer();
		response_.set_msg_type (msg_type);
		response_.set_token (reinterpret_cast<uintptr_t> (request_token));
		response_.set_encoded_buffer (buffer.c_buf(), buffer.size());
		int rc = zmq_msg_init_size (&msg_, response_.ByteSize());
		CHECK(0 == rc);
		response_.SerializeToArray (zmq_msg_data (&msg_), static_cast<int> (zmq_msg_size (&msg_)));
		rc = zmq_send (response_sock_.get(), &msg_, 0);
		CHECK(0 == rc);
		rc = zmq_msg_close (&msg_);
		CHECK(0 == rc);
	}
	
/* worker unique identifier */
	const unsigned id_;
	std::string prefix_;

/* 0mq context */
	std::shared_ptr<void> zmq_context_;

/* Socket to receive refresh requests on. */
	std::shared_ptr<void> request_sock_;

/* response socket for sending images. */
	std::shared_ptr<void> response_sock_;

/* Incoming 0mq message */
	zmq_msg_t msg_;
	provider::Request request_;

/* Outgoing rfa message */
	rfa::message::RespMsg response_msg_;
	provider::Response response_;

/* Publish fields. */
	std::shared_ptr<rfa::data::FieldList> fields_;	/* no copy ctor: cannot use unique_ptr */
	rfa::message::AttribInfo attribInfo_;
	rfa::common::RespStatus status_;

/* Iterator for populating publish fields */
	std::shared_ptr<rfa::data::SingleWriteIterator> single_write_it_;	/* no copy ctor */

/* application reference */
	std::shared_ptr<provider_t> provider_;
	const config_t& config_;
};

usagi::usagi_t::usagi_t()
	: response_ (false),
	  fields_ (false),
	  attribInfo_ (false)
{
/* constant across publishes */
	fields_.setInfo (kDictionaryId, kFieldListId);
}

usagi::usagi_t::~usagi_t()
{
	LOG(INFO) << "fin.";
}

int
usagi::usagi_t::Run ()
{
	LOG(INFO) << config_;

	try {
/* ZeroMQ context. */
		std::function<int(void*)> zmq_term_deleter = zmq_term;
		zmq_context_.reset (zmq_init (0), zmq_term_deleter);
		CHECK((bool)zmq_context_);

/* pull for RFA responses */
		std::function<int(void*)> zmq_close_deleter = zmq_close;
		response_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PULL), zmq_close_deleter);
		CHECK((bool)response_sock_);
		int rc = zmq_bind (response_sock_.get(), "inproc://usagi/rfa/response");
		CHECK(0 == rc);
	} catch (const std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	try {
/* RFA context. */
		rfa_.reset (new rfa_t (config_));
		if (!(bool)rfa_ || !rfa_->Init())
			goto cleanup;

/* RFA asynchronous event queue. */
		const RFA_String eventQueueName (config_.event_queue_name.c_str(), 0, false);
		event_queue_.reset (rfa::common::EventQueue::create (eventQueueName), std::mem_fun (&rfa::common::EventQueue::destroy));
		if (!(bool)event_queue_)
			goto cleanup;

/* RFA logging. */
		log_.reset (new logging::LogEventProvider (config_, event_queue_));
		if (!(bool)log_ || !log_->Register())
			goto cleanup;

/* RFA provider. */
		provider_.reset (new provider_t (config_, rfa_, event_queue_, zmq_context_));
		if (!(bool)provider_ || !provider_->Init())
			goto cleanup;

/* Create state for published RIC. */
		static const std::string msft ("MSFT.O");
		auto stream = std::make_shared<broadcast_stream_t> ();
		if (!(bool)stream)
			goto cleanup;
		if (!provider_->CreateItemStream (msft.c_str(), stream))
			goto cleanup;
		msft_stream_ = std::move (stream);

/* Pre-allocate memory buffer for payload iterator */
		CHECK (config_.maximum_data_size > 0);
		single_write_it_.initialize (fields_, (uint32_t)config_.maximum_data_size);
		CHECK (single_write_it_.isInitialized());

	} catch (const rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "InvalidUsageException: { "
			  "\"Severity\": \"" << severity_string (e.getSeverity()) << "\""
			", \"Classification\": \"" << classification_string (e.getClassification()) << "\""
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\" }";
		goto cleanup;
	} catch (const rfa::common::InvalidConfigurationException& e) {
		LOG(ERROR) << "InvalidConfigurationException: { "
			  "\"Severity\": \"" << severity_string (e.getSeverity()) << "\""
			", \"Classification\": \"" << classification_string (e.getClassification()) << "\""
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
			", \"ParameterName\": \"" << e.getParameterName() << "\""
			", \"ParameterValue\": \"" << e.getParameterValue() << "\" }";
		goto cleanup;
	}

	try {
		std::function<int(void*)> zmq_close_deleter = zmq_close;
/* Worker abort socket. */
		worker_abort_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)worker_abort_sock_);
		int rc = zmq_bind (worker_abort_sock_.get(), "inproc://usagi/worker/abort");
		CHECK(0 == rc);
/* Worker threads. */
		for (size_t i = 0; i < config_.worker_count; ++i) {
			const unsigned worker_id = (unsigned)(1 + i);
			LOG(INFO) << "Spawning worker #" << worker_id;
			auto worker = std::make_shared<worker_t> (provider_, config_, zmq_context_, worker_id);
			if (!(bool)worker)
				goto cleanup;
			auto thread = std::make_shared<boost::thread> ([worker](){ if (worker->Init()) worker->Run(); });
			if (!(bool)thread)
				goto cleanup;
			workers_.emplace_front (std::make_pair (worker, thread));
		}
	} catch (const std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	try {
/* Timer for demo periodic publishing of items. */
		timer_.reset (new time_pump_t<boost::chrono::system_clock> (boost::chrono::system_clock::now(), boost::chrono::seconds (1), this));
		if (!(bool)timer_)
			goto cleanup;
		timer_thread_.reset (new boost::thread ([this](){ timer_->Run(); }));
		if (!(bool)timer_thread_)
			goto cleanup;
		LOG(INFO) << "Added periodic timer, interval " << boost::chrono::seconds (1).count() << " seconds";
	} catch (const std::exception& e) {
		LOG(ERROR) << "Timer::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	LOG(INFO) << "Init complete, entering main loop.";
	MainLoop ();
	LOG(INFO) << "Main loop terminated, cleaning up.";
	Clear();
	return EXIT_SUCCESS;
cleanup:
	LOG(INFO) << "Init failed, cleaning up.";
	Clear();
	return EXIT_FAILURE;
}

/* On a shutdown event set a global flag and force the event queue
 * to catch the event by submitting a log event.
 */
static
BOOL
CtrlHandler (
	DWORD	fdwCtrlType
	)
{
	const char* message;
	switch (fdwCtrlType) {
	case CTRL_C_EVENT:
		message = "Caught ctrl-c event, shutting down";
		break;
	case CTRL_CLOSE_EVENT:
		message = "Caught close event, shutting down";
		break;
	case CTRL_BREAK_EVENT:
		message = "Caught ctrl-break event, shutting down";
		break;
	case CTRL_LOGOFF_EVENT:
		message = "Caught logoff event, shutting down";
		break;
	case CTRL_SHUTDOWN_EVENT:
	default:
		message = "Caught shutdown event, shutting down";
		break;
	}
/* if available, raise event on global abort socket to break out of zmq_poll. */
	if (INVALID_SOCKET != g_abort_sock) {
		const char one = '1';
		send (g_abort_sock, &one, sizeof (one), 0);
	}
	LOG(INFO) << message;
	return TRUE;
}

class rfa_dispatcher_t : public rfa::common::DispatchableNotificationClient
{
public:
	rfa_dispatcher_t (std::shared_ptr<void> zmq_context) : zmq_context_ (zmq_context) {}
	~rfa_dispatcher_t()
	{
		CHECK (event_sock_.use_count() <= 1);
		event_sock_.reset();
		zmq_context_.reset();
	}

	int Init (void)
	{
		std::function<int(void*)> zmq_close_deleter = zmq_close;
		CHECK((bool)zmq_context_);
		event_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)event_sock_);
		return zmq_connect (event_sock_.get(), "inproc://usagi/rfa/event");
	}

	void notify (rfa::common::Dispatchable& eventSource, void* closure) override
	{
		DCHECK((bool)event_sock_);
		int rc = zmq_msg_init_size (&msg_, 0);
		CHECK(0 == rc);
		rc = zmq_send (event_sock_.get(), &msg_, 0);
		CHECK(0 == rc);
		rc = zmq_msg_close (&msg_);
		CHECK(0 == rc);
	}

protected:
	std::shared_ptr<void> zmq_context_;
	std::shared_ptr<void> event_sock_;
	zmq_msg_t msg_;
};

void
usagi::usagi_t::MainLoop()
{
	std::function<int(void*)> zmq_close_deleter = zmq_close;
	rfa_dispatcher_t dispatcher (zmq_context_);
	zmq_pollitem_t items[3];
	SOCKET s[2];
	std::shared_ptr<void> event_sock;
	zmq_msg_t msg;
	rfa::message::RespMsg response_msg (false);
	rfa::sessionLayer::RequestToken* request_token;
	provider::Response response;

/* pull RFA events */
	event_sock.reset (zmq_socket (zmq_context_.get(), ZMQ_PULL), zmq_close_deleter);
	CHECK((bool)event_sock);
	int rc = zmq_bind (event_sock.get(), "inproc://usagi/rfa/event");
	CHECK(0 == rc);
	items[0].socket = event_sock.get();
	items[0].events = ZMQ_POLLIN;

/* pull RFA responses */
	items[1].socket = response_sock_.get();
	items[1].events = ZMQ_POLLIN;

/* pull abort event */
/* use loopback sockets to simulate a pipe suitable for win32/select() */
        struct sockaddr_in addr;
        SOCKET listener;
        int sockerr;
        int addrlen = sizeof (addr);
        unsigned long one = 1;

	s[0] = s[1] = INVALID_SOCKET;

        listener = socket (AF_INET, SOCK_STREAM, 0);
        assert (listener != INVALID_SOCKET);

	ZeroMemory (&addr, sizeof (addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr ("127.0.0.1");
        assert (addr.sin_addr.s_addr != INADDR_NONE);

        sockerr = ::bind (listener, (const struct sockaddr*)&addr, sizeof (addr));
        assert (sockerr != SOCKET_ERROR);

        sockerr = getsockname (listener, (struct sockaddr*)&addr, &addrlen);
        assert (sockerr != SOCKET_ERROR);

// Listen for incoming connections.
        sockerr = listen (listener, 1);
        assert (sockerr != SOCKET_ERROR);

// Create the socket.
        s[1] = WSASocket (AF_INET, SOCK_STREAM, 0, NULL, 0, 0);
        assert (s[1] != INVALID_SOCKET);

// Connect to the remote peer.
        sockerr = connect (s[1], (struct sockaddr*)&addr, addrlen);
/* Failure may be delayed from bind and may be due to socket exhaustion as explained
 * in MSDN(bind Function).
 */
        assert (sockerr != SOCKET_ERROR);

// Accept connection.
        s[0] = accept (listener, NULL, NULL);
        assert (s[0] != INVALID_SOCKET);

// Set read-end to non-blocking mode
        sockerr = ioctlsocket (s[0], FIONBIO, &one);
        assert (sockerr != SOCKET_ERROR);

// We don't need the listening socket anymore. Close it.
        sockerr = closesocket (listener);
        assert (sockerr != SOCKET_ERROR);

	items[2].socket = nullptr;
	items[2].fd     = s[0];
	items[2].events = ZMQ_POLLIN;

	rc = dispatcher.Init();
	CHECK(0 == rc);
	event_queue_->registerNotificationClient (dispatcher, nullptr);

/* Add shutdown handler. */
	g_abort_sock = s[1];
	::SetConsoleCtrlHandler ((PHANDLER_ROUTINE)::CtrlHandler, TRUE);

	do {
		int rc = zmq_poll (items, _countof (items), -1);
		if (rc <= 0)
			continue;
/* #0 - RFA event */
		if (0 != (items[0].revents & ZMQ_POLLIN))
			event_queue_->dispatch (rfa::common::Dispatchable::NoWait);
/* #1 - RFA response message */
		if (0 != (items[1].revents & ZMQ_POLLIN))
		{
			rc = zmq_msg_init (&msg);
			CHECK(0 == rc);
			rc = zmq_recv (items[1].socket, &msg, 0);
			CHECK(0 == rc);
			if (!response.ParseFromArray (zmq_msg_data (&msg), static_cast<int> (zmq_msg_size (&msg)))) {
				LOG(ERROR) << "Received invalid response.";
				rc = zmq_msg_close (&msg);
				CHECK(0 == rc);
				continue;
			}

			DVLOG(1) << response.DebugString();
/* token */
			const uintptr_t token = response.token();
			request_token = reinterpret_cast<rfa::sessionLayer::RequestToken*> (token);
			try {
				LOG(INFO) << "response size: " << response.encoded_buffer().size();
/* encoded buffer */
				rfa::common::Buffer buffer (const_cast<unsigned char*> (reinterpret_cast<const unsigned char*> (response.encoded_buffer().c_str())),
							    static_cast<int> (response.encoded_buffer().size()),
							    static_cast<int> (response.encoded_buffer().size()),
							    false);
				response_msg.setEncodedBuffer (buffer);
/* forward to RFA */
				provider_->Submit (&response_msg, request_token, nullptr);
				LOG(INFO) << "response sent.";
				response_msg.clear();
/* only if successful */
				if (response.msg_type() == provider::Response::MSG_INITIAL)
				{
					auto sp = provider_->GetRequest (request_token);
					if ((bool)sp) {
						sp->has_initial_image.store (true);
						LOG(INFO) << "Enabled stream updates.";
					}
				}
			} catch (rfa::common::InvalidUsageException& e) {
				LOG(ERROR) << "EncodedBuffer::InvalidUsageException: { " <<
						"\"StatusText\": \"" << e.getStatus().getStatusText() << "\""
						" }";
			}
			rc = zmq_msg_close (&msg);
			CHECK(0 == rc);
		}
/* #2 - Abort request */
	} while (0 == (items[2].revents & ZMQ_POLLIN));

/* Remove shutdown handler. */
	::SetConsoleCtrlHandler ((PHANDLER_ROUTINE)::CtrlHandler, FALSE);

/* cleanup */
	event_queue_->unregisterNotificationClient (dispatcher);

	closesocket (s[0]);
	closesocket (s[1]);

	CHECK (event_sock.use_count() <= 1);
	event_sock.reset();
}

void
usagi::usagi_t::Clear()
{
/* Stop generating new events. */
	if (timer_thread_) {
		timer_thread_->interrupt();
		timer_thread_->join();
	}	
	timer_thread_.reset();
	timer_.reset();

/* Interrupt worker threads. */
	if (!workers_.empty()) {
		LOG(INFO) << "Reviewing worker threads.";
		provider::Request request;
		request.set_msg_type (provider::Request::MSG_ABORT);
		zmq_msg_t msg;
		unsigned active_threads = 0;
		for (auto it = workers_.begin(); it != workers_.end(); ++it) {
			if ((bool)it->second && it->second->joinable()) {
				zmq_msg_init_size (&msg, request.ByteSize());
				request.SerializeToArray (zmq_msg_data (&msg), (int)zmq_msg_size (&msg));
				zmq_send (worker_abort_sock_.get(), &msg, 0);
				zmq_msg_close (&msg);
				++active_threads;
			}
		}
		if (active_threads > 0) {
			LOG(INFO) << "Sending interrupt to " << active_threads << " worker threads.";
			for (auto it = workers_.begin(); it != workers_.end(); ++it) {
				if ((bool)it->second && it->second->joinable())
					it->second->join();
				LOG(INFO) << "Thread #" << it->first->GetId() << " joined.";
				it->first.reset();
			}
		}
		LOG(INFO) << "All worker threads joined.";
	}

/* Signal message pump thread to exit. */
	if ((bool)event_queue_) {
		event_queue_->deactivate();
	}

	msft_stream_.reset();

/* Shutdown final connecting 0mq users before bringing down bound sockets. */
	CHECK (provider_.use_count() <= 1);
	provider_.reset();
	CHECK (worker_abort_sock_.use_count() <= 1);
	worker_abort_sock_.reset();
	CHECK (response_sock_.use_count() <= 1);
	response_sock_.reset();
/* Bring down 0mq context as all sockets are closed. */
	CHECK (zmq_context_.use_count() <= 1);
	zmq_context_.reset();
/* Release everything with an RFA dependency. */
	CHECK (log_.use_count() <= 1);
	log_.reset();
	CHECK (event_queue_.use_count() <= 1);
	event_queue_.reset();
	CHECK (rfa_.use_count() <= 1);
	rfa_.reset();
}

bool
usagi::usagi_t::OnTimer (
	const boost::chrono::time_point<boost::chrono::system_clock>& t
	)
{
/* calculate timer accuracy, typically 15-1ms with default timer resolution.
 */
	if (DLOG_IS_ON(INFO)) {
		using namespace boost::chrono;
		auto now = system_clock::now();
		auto ms = duration_cast<milliseconds> (now - t);
		if (0 == ms.count()) {
			LOG(INFO) << "delta " << duration_cast<microseconds> (now - t).count() << "us";
		} else {
			LOG(INFO) << "delta " << ms.count() << "ms";
		}
	}

	try {
		SendRefresh();
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "InvalidUsageException: { "
			  "\"Severity\": \"" << severity_string (e.getSeverity()) << "\""
			", \"Classification\": \"" << classification_string (e.getClassification()) << "\""
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\" }";
	}
/* continue raising timer events */
	return true;
}

bool
usagi::usagi_t::SendRefresh()
{
/* 7.4.8.1 Create a response message (4.2.2) */
	response_.clear();

/* 7.4.8.2 Create or re-use a request attribute object (4.2.4) */
	attribInfo_.clear();
	attribInfo_.setNameType (rfa::rdm::INSTRUMENT_NAME_RIC);
	attribInfo_.setName (msft_stream_->rfa_name);
	attribInfo_.setServiceID (provider_->GetServiceId());

/* 7.4.8.3 Set the message model type of the response. */
	response_.setMsgModelType (rfa::rdm::MMT_MARKET_PRICE);
/* 7.4.8.4 Set response type, response type number, and indication mask. */
	response_.setRespType (rfa::message::RespMsg::UpdateEnum);
	response_.setRespTypeNum (rfa::rdm::REFRESH_UNSOLICITED);
	response_.setIndicationMask (rfa::message::RespMsg::DoNotFilterFlag | rfa::message::RespMsg::DoNotRippleFlag);

/* 4.3.1 RespMsg.Payload */
// not std::map :(  derived from rfa::common::Data
	const uint16_t rwf_version = provider_->GetRwfVersion();
	fields_.setAssociatedMetaInfo (rwf_version / 256, rwf_version % 256);

/* Clear required for SingleWriteIterator state machine. */
	auto& it = single_write_it_;
	DCHECK (it.isInitialized());
	it.clear();
	it.start (fields_);

	rfa::data::FieldEntry field (false);
	field.setFieldID (kRdmTradePriceId);
	it.bind (field);
	it.setReal (++msft_stream_->count, rfa::data::Exponent0);

	it.complete();
/* Set a reference to field list, not a copy */
	response_.setPayload (fields_);

#ifdef DEBUG
/* 4.2.8 Message Validation.  RFA provides an interface to verify that
 * constructed messages of these types conform to the Reuters Domain
 * Models as specified in RFA API 7 RDM Usage Guide.
 */
	uint8_t validation_status = rfa::message::MsgValidationError;
	try {
		RFA_String warningText;
		validation_status = response.validateMsg (&warningText);
		if (rfa::message::MsgValidationWarning == validation_status)
			LOG(ERROR) << prefix_ << "MMT_MARKET_PRICE::validateMsg: { \"warningText\": \"" << warningText << "\" }";
	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "MMT_MARKET_PRICE::InvalidUsageException: { " <<
				   response_ <<
				", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
#endif

	provider_->Send (msft_stream_.get(), &response_, attribInfo_);
	LOG(INFO) << "Sent refresh.";
	return true;
}

/* eof */