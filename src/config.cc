/* User-configurable settings.
 */

#include "config.hh"

static const char* kDefaultRsslPort = "14002";

usagi::config_t::config_t() :
/* default values */
	service_name ("VTA"),
	rssl_default_port (kDefaultRsslPort),
	session_name ("SessionName"),
	monitor_name ("ApplicationLoggerMonitorName"),
	event_queue_name ("EventQueueName"),
	connection_name ("ConnectionName"),
	publisher_name ("PublisherName"),
	vendor_name ("VendorName"),
	session_capacity (2)
{
/* C++11 initializer lists not supported in MSVC2010 */
}

/* eof */
