import rpyc
from rpyc.utils.server import ThreadedServer


class ProxyService(rpyc.Service):
    class exposed_Proxy(object):

        # proxy should know all
        # proxy caches the main master port
        main_master_port = 0
        master_ports = []
        minion_ports = []

        # try the current master. If down, get a new master
        def exposed_get_master(self):
            if self.main_master_port is 0:
                return self.set_main_master()
            else:
                try:
                    rpyc.connect("127.0.0.1", port=self.main_master_port)
                    return self.main_master_port
                except Exception as e:
                    print(e)
                    print("[Proxy]:Master down! Use another master!")
                    return self.set_main_master()

        # set a new master as main master
        def set_main_master(self):
            for master_port in self.master_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=master_port)
                    master = con.root.Master()

                    # set all the ports
                    master.set_minion_ports(self.minion_ports)
                    self.main_master_port = master_port

                    return self.main_master_port
                except ConnectionRefusedError:
                    # dead master. Just continue
                    continue


def set_conf(master_ports, minions_ports):
    # master should know everyone's port
    proxy = ProxyService.exposed_Proxy
    proxy.master_ports = master_ports
    proxy.minion_ports = minions_ports


def startProxyService(proxy_port, master_ports, minions_ports):
    set_conf(master_ports, minions_ports)
    t = ThreadedServer(ProxyService, port=proxy_port)
    t.start()
