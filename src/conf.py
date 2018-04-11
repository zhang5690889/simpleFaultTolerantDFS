# This file contains all default configuration for all components to read from
# Note: this is the single source of truth about the whole system.

# Master server configuration
replication_factor = 3

# Default values for starting all services
default_minion_ports = [8898, 8890, 8891, 5534, 5553]
default_master_ports = [2131, 2121, 2136]


default_proxy_port = 2130
