/* RFA interactive publisher pretending to be a broadcast provider.
 *
 * An interactive provider sits listening on a port for RSSL connections,
 * once a client is connected requests may be submitted for snapshots or
 * subscriptions to item streams.  This application will broadcast updates
 * continuously independent of client interest and the provider side will
 * perform fan-out as required.
 *
 * The provider is not required to perform last value caching, forcing the
 * client to wait for a subsequent broadcast to actually see data.
 */

#ifndef __USAGI_HH__
#define __USAGI_HH__
#pragma once

#include <cstdint>
#include <forward_list>
#include <memory>

/* Boost Chrono. */
#include <boost/chrono.hpp>

/* Boost noncopyable base class */
#include <boost/utility.hpp>

/* Boost threading. */
#include <boost/thread.hpp>

/* RFA 7.2 */
#include <rfa/rfa.hh>

#include "chromium/logging.hh"

#include "config.hh"
#include "provider.hh"

namespace logging
{
	class LogEventProvider;
}

namespace usagi
{
	class rfa_t;
	class provider_t;
	class worker_t;

/* Basic example structure for application state of an item stream. */
	class broadcast_stream_t : public item_stream_t
	{
	public:
		broadcast_stream_t () :
			count (0)
		{
		}

		uint64_t	count;
	};

/* Periodic timer event source */
	template<class Clock, class Duration = typename Clock::duration>
	class time_base_t
	{
	public:
		virtual bool OnTimer (const boost::chrono::time_point<Clock, Duration>& t) = 0;
	};

	template<class Clock, class Duration = typename Clock::duration>
	class time_pump_t
	{
	public:
		time_pump_t (const boost::chrono::time_point<Clock, Duration>& due_time, Duration td, time_base_t<Clock, Duration>* cb) :
			due_time_ (due_time),
			td_ (td),
			cb_ (cb)
		{
			CHECK(nullptr != cb_);
		}

		void Run (void)
		{
			try {
				while (true) {
					boost::this_thread::sleep_until (due_time_);
					if (!cb_->OnTimer (due_time_))
						break;
					due_time_ += td_;
				}
			} catch (boost::thread_interrupted const&) {
				LOG(INFO) << "Timer thread interrupted.";
			}
		}

	private:
		boost::chrono::time_point<Clock, Duration> due_time_;
		Duration td_;
		time_base_t<Clock, Duration>* cb_;
	};

	class usagi_t :
		public time_base_t<boost::chrono::system_clock>,
		boost::noncopyable
	{
	public:
		usagi_t();
		~usagi_t();

/* Run the provider with the given command-line parameters.
 * Returns the error code to be returned by main().
 */
		int Run();
		void Clear();

/* Configured period timer entry point. */
		bool OnTimer (const boost::chrono::time_point<boost::chrono::system_clock>& t) override;

	private:

/* Run core event loop. */
		void MainLoop();

/* Broadcast out message. */
		bool SendRefresh() throw (rfa::common::InvalidUsageException);

/* Application configuration. */
		config_t config_;

/* RFA context. */
		std::shared_ptr<rfa_t> rfa_;

/* RFA asynchronous event queue. */
		std::shared_ptr<rfa::common::EventQueue> event_queue_;

/* RFA logging */
		std::shared_ptr<logging::LogEventProvider> log_;

/* RFA provider */
		std::shared_ptr<provider_t> provider_;
	
/* Item stream. */
		std::shared_ptr<broadcast_stream_t> msft_stream_;

/* Publish state. */
		rfa::message::RespMsg response_;
		rfa::data::FieldList fields_;
		rfa::message::AttribInfo attribInfo_;
		rfa::common::RespStatus status_;

/* Iterator for populating publish fields */
		rfa::data::SingleWriteIterator single_write_it_;

/* Thread timer. */
		std::unique_ptr<time_pump_t<boost::chrono::system_clock>> timer_;
		std::unique_ptr<boost::thread> timer_thread_;

/* RFA request thread workers. */
		std::forward_list<std::pair<std::shared_ptr<worker_t>, std::shared_ptr<boost::thread>>> workers_;

/* Thread worker shutdown socket. */
		std::shared_ptr<void> zmq_context_;
		std::shared_ptr<void> worker_abort_sock_;

/* Response socket. */
		std::shared_ptr<void> response_sock_;
	};

} /* namespace usagi */

#endif /* __USAGI_HH__ */

/* eof */