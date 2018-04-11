# This file defines a proxy server as the entry point of the entire system
# Assumption: proxy server never goes down.
# Some Thoughts: In production environments, "proxy server" can be a cluster.

import rpyc
from rpyc.utils.server import ThreadedServer


class ProxyService(rpyc.Service):
    class exposed_Proxy(object):

        main_master = None
        master_ports = []
        minion_ports = []

        def exposed_get_master(self):
            if self.main_master is None:
                return self.set_main_master()
            else:
                try:
                    self.con = rpyc.connect("127.0.0.1", port=self.main_master)
                    return self.con.root.Master()
                except Exception as e:
                    print ("[Proxy]:Master down! Use another master!")
                    return self.set_main_master()


        def set_main_master(self):
            for master_port in self.master_ports:
                try:
                    self.con = rpyc.connect("127.0.0.1", port=master_port)
                    self.master = self.con.root.Master()
                    minion_ports = self.minion_ports

                    self.master.get_minion_ports(minion_ports)
                    self.main_master=self.master
                    return self.main_master
                except ConnectionRefusedError:
                    # dead master. Just continue
                    continue




def set_conf(master_ports, minions_ports):

    proxy = ProxyService.exposed_Proxy
    proxy.master_ports = master_ports
    proxy.minion_ports = minions_ports


def startProxyService(proxy_port, master_ports, minions_ports):
    set_conf(master_ports, minions_ports)
    t = ThreadedServer(ProxyService, port=proxy_port)
    t.start()
