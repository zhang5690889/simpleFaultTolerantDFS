# Make sure you kill k - 1 nodes. there are still at least k nodes live. otherwise it will blow up

# Master server configuration
replication_factor = 3

# Default values for starting all services
default_minion_ports = [8898, 8890, 8891, 5534, 5553, 5536]
default_master_ports = [2131, 2121]

default_proxy_port = 2130
