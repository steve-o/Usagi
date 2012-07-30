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

static std::weak_ptr<rfa::common::EventQueue> g_event_queue;

class usagi::worker_t
{
public:
	worker_t (std::shared_ptr<void>& context, unsigned id, std::shared_ptr<provider_t> provider) :
		context_ (context),
		id_ (id),
		provider_ (provider)
	{
	}

	void operator()()
	{
		zmq_msg_t msg;
		int rc;

		std::function<int(void*)> zmq_close_deleter = zmq_close;
/* Socket to receive refresh requests on. */
		std::shared_ptr<void> receiver;
		receiver.reset (zmq_socket (context_.get(), ZMQ_PULL), zmq_close_deleter);
		CHECK((bool)receiver);
		rc = zmq_bind (receiver.get(), "inproc://usagi/refresh");
		CHECK(0 == rc);
/* Also bind for terminating interrupt. */
		rc = zmq_connect (receiver.get(), "inproc://usagi/abort");
		CHECK(0 == rc);

		LOG(INFO) << "Thread #" << id_ << ": Accepting refresh requests.";

		while (true) {
/* Receive new request. */
			rc = zmq_msg_init (&msg);
			CHECK(0 == rc);
			LOG(INFO) << "Thread #" << id_ << ": Awaiting new job.";
			rc = zmq_recv (receiver.get(), &msg, 0);
			CHECK(0 == rc);
			std::string job_text;
			if (zmq_msg_size (&msg) > 0)
				job_text.assign (static_cast<char*> (zmq_msg_data (&msg)), zmq_msg_size (&msg));
			LOG(INFO) << "Thread #" << id_ << ": Received job \"" << job_text << "\"";
			rc = zmq_msg_close (&msg);
			CHECK(0 == rc);

			if (0 == job_text.compare ("abort"))
				break;
		}

		LOG(INFO) << "Thread #" << id_ << ": Worker closed.";
	}

protected:
	const unsigned id_;
	std::shared_ptr<void> context_;
	std::shared_ptr<provider_t> provider_;
};

usagi::usagi_t::~usagi_t()
{
	LOG(INFO) << "fin.";
}

int
usagi::usagi_t::run ()
{
	LOG(INFO) << config_;

	try {
/* ZeroMQ context. */
		std::function<int(void*)> zmq_term_deleter = zmq_term;
		zmq_context_.reset (zmq_init (0), zmq_term_deleter);
		CHECK((bool)zmq_context_);
	} catch (std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	try {
/* RFA context. */
		rfa_.reset (new rfa_t (config_));
		if (!(bool)rfa_ || !rfa_->init())
			goto cleanup;

/* RFA asynchronous event queue. */
		const RFA_String eventQueueName (config_.event_queue_name.c_str(), 0, false);
		event_queue_.reset (rfa::common::EventQueue::create (eventQueueName), std::mem_fun (&rfa::common::EventQueue::destroy));
		if (!(bool)event_queue_)
			goto cleanup;
/* Create weak pointer to handle application shutdown. */
		g_event_queue = event_queue_;

/* RFA logging. */
		log_.reset (new logging::LogEventProvider (config_, event_queue_));
		if (!(bool)log_ || !log_->Register())
			goto cleanup;

/* RFA provider. */
		provider_.reset (new provider_t (config_, rfa_, event_queue_, zmq_context_));
		if (!(bool)provider_ || !provider_->init())
			goto cleanup;

/* Create state for published RIC. */
		static const std::string msft ("MSFT.O");
		auto stream = std::make_shared<broadcast_stream_t> ();
		if (!(bool)stream)
			goto cleanup;
		if (!provider_->createItemStream (msft.c_str(), stream))
			goto cleanup;
		msft_stream_ = std::move (stream);

/* Pre-allocate memory buffer for payload iterator */
		CHECK (config_.maximum_data_size > 0);
		single_write_it_.initialize (fields_, (uint32_t)config_.maximum_data_size);
		CHECK (single_write_it_.isInitialized());

	} catch (rfa::common::InvalidUsageException& e) {
		LOG(ERROR) << "InvalidUsageException: { "
			  "\"Severity\": \"" << severity_string (e.getSeverity()) << "\""
			", \"Classification\": \"" << classification_string (e.getClassification()) << "\""
			", \"StatusText\": \"" << e.getStatus().getStatusText() << "\" }";
		goto cleanup;
	} catch (rfa::common::InvalidConfigurationException& e) {
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
		abort_sock_.reset (zmq_socket (zmq_context_.get(), ZMQ_PUSH), zmq_close_deleter);
		CHECK((bool)abort_sock_);
		int rc = zmq_bind (abort_sock_.get(), "inproc://usagi/abort");
		CHECK(0 == rc);
/* Worker threads. */
		for (size_t i = 0; i < config_.worker_count; ++i) {
			const unsigned worker_id = (unsigned)(1 + i);
			LOG(INFO) << "Spawning worker #" << worker_id;
			auto worker = std::make_shared<worker_t> (zmq_context_, worker_id, provider_);
			if (!(bool)worker)
				goto cleanup;
			auto thread = std::make_shared<boost::thread> (*worker.get());
			if (!(bool)thread)
				goto cleanup;
			workers_.emplace_front (std::make_pair (worker, thread));
		}
	} catch (std::exception& e) {
		LOG(ERROR) << "ZeroMQ::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	try {
/* Timer for demo periodic publishing of items. */
		timer_.reset (new time_pump_t<boost::chrono::system_clock> (boost::chrono::system_clock::now(), boost::chrono::seconds (1), this));
		if (!(bool)timer_)
			goto cleanup;
		timer_thread_.reset (new boost::thread (*timer_.get()));
		if (!(bool)timer_thread_)
			goto cleanup;
		LOG(INFO) << "Added periodic timer, interval " << boost::chrono::seconds (1).count() << " seconds";
	} catch (std::exception& e) {
		LOG(ERROR) << "Timer::Exception: { "
			"\"What\": \"" << e.what() << "\" }";
		goto cleanup;
	}

	LOG(INFO) << "Init complete, entering main loop.";
	mainLoop ();
	LOG(INFO) << "Main loop terminated, cleaning up.";
	clear();
	return EXIT_SUCCESS;
cleanup:
	LOG(INFO) << "Init failed, cleaning up.";
	clear();
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
/* if available, deactivate global event queue pointer to break running loop. */
	if (!g_event_queue.expired()) {
		auto sp = g_event_queue.lock();
		sp->deactivate();
	}
	LOG(INFO) << message;
	return TRUE;
}

void
usagi::usagi_t::mainLoop()
{
/* Add shutdown handler. */
	::SetConsoleCtrlHandler ((PHANDLER_ROUTINE)::CtrlHandler, TRUE);
	while (event_queue_->isActive()) {
		event_queue_->dispatch (rfa::common::Dispatchable::InfiniteWait);
	}
/* Remove shutdown handler. */
	::SetConsoleCtrlHandler ((PHANDLER_ROUTINE)::CtrlHandler, FALSE);
}

void
usagi::usagi_t::clear()
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
		LOG(INFO) << "Sending interrupt to worker threads.";
		zmq_msg_t abort_msg;
		zmq_msg_init_data (&abort_msg, "abort", strlen ("abort"), nullptr, nullptr);
		zmq_send (abort_sock_.get(), &abort_msg, 0);
		zmq_msg_close (&abort_msg);
		LOG(INFO) << "Awaiting worker threads to terminate.";
		for (auto it = workers_.begin(); it != workers_.end(); ++it) {
			if ((bool)it->second) it->second->join();
			it->first.reset();
		}
		LOG(INFO) << "All worker threads joined.";
	}
	abort_sock_.reset();
	zmq_context_.reset();

/* Signal message pump thread to exit. */
	if ((bool)event_queue_)
		event_queue_->deactivate();

	msft_stream_.reset();

/* Release everything with an RFA dependency. */
	assert (provider_.use_count() <= 1);
	provider_.reset();
	assert (log_.use_count() <= 1);
	log_.reset();
	assert (event_queue_.use_count() <= 1);
	event_queue_.reset();
	assert (rfa_.use_count() <= 1);
	rfa_.reset();
}

bool
usagi::usagi_t::processTimer (
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
		sendRefresh();
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
usagi::usagi_t::sendRefresh()
{
/* 7.5.9.1 Create a response message (4.2.2) */
	rfa::message::RespMsg response (false);	/* reference */

/* 7.5.9.2 Set the message model type of the response. */
	response.setMsgModelType (rfa::rdm::MMT_MARKET_PRICE);
/* 7.5.9.3 Set response type. */
	response.setRespType (rfa::message::RespMsg::UpdateEnum);
/* 7.5.9.4 Set the response type enumation. */
	response.setRespTypeNum (rfa::rdm::REFRESH_UNSOLICITED);

/* 7.5.9.5 Create or re-use a request attribute object (4.2.4) */
	attribInfo_.clear();
	attribInfo_.setNameType (rfa::rdm::INSTRUMENT_NAME_RIC);
	attribInfo_.setName (msft_stream_->rfa_name);
	attribInfo_.setServiceID (provider_->getServiceId());

/* 4.3.1 RespMsg.Payload */
// not std::map :(  derived from rfa::common::Data
	fields_.setAssociatedMetaInfo (provider_->getRwfMajorVersion(), provider_->getRwfMinorVersion());
	fields_.setInfo (kDictionaryId, kFieldListId);

/* Clear required for SingleWriteIterator state machine. */
	auto& it = single_write_it_;
	DCHECK (it.isInitialized());
	it.clear();
	it.start (fields_);

	rfa::data::FieldEntry field (false);

	field.setFieldID (kRdmRdnDisplayId);
	it.bind (field);
	it.setUInt (100);

	field.setFieldID (kRdmTradePriceId);
	it.bind (field);
	it.setReal (++msft_stream_->count, rfa::data::Exponent0);

	it.complete();
/* Set a reference to field list, not a copy */
	response.setPayload (fields_);

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
				   response <<
				", \"StatusText\": \"" << e.getStatus().getStatusText() << "\""
				" }";
	}
#endif

	provider_->send (*msft_stream_.get(), response, attribInfo_);
	LOG(INFO) << "Sent refresh.";
	return true;
}

/* eof */