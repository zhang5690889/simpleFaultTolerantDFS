import rpyc


class client:
    # client needs to know proxy's port
    def __init__(self, proxy_port_num):
        self.con = rpyc.connect('localhost', proxy_port_num)
        self.proxy = self.con.root.Proxy()

    # Client public API : get, put, delete
    def get(self, key):
        master_port = self.proxy.get_master()
        try:
            con = rpyc.connect("127.0.0.1", port=master_port)
            master = con.root.Master()
            minion_ports = master.get_minion_that_has_the_key(key)

            # get value from each
            for minion_port in minion_ports:
                con = rpyc.connect("127.0.0.1", port=minion_port)
                minion = con.root.Minion()
                data = minion.get_data_by_key(key)
                return data
            return ''
        # Upon any possible exception, retry
        except Exception:
            # if client cannot connect to master, just keep retrying
            self.get(key)

    def put(self, source, key):
        # get allocation scheme from master through proxy
        try:
            master_port = self.proxy.get_master()
            con = rpyc.connect("127.0.0.1", port=master_port)
            master = con.root.Master()
            minion_ports = master.get_allocation_scheme()

            with open(source) as f:
                data = f.read()
                return self.send_to_minion(minion_ports, data, key)


        # Upon any possible exception, retry
        except Exception:
            # if client cannot connect to master, just keep retrying
            self.put(source, key)

    def delete(self, key):
        # delete race condition
        master_port = self.proxy.get_master()
        con = rpyc.connect("127.0.0.1", port=master_port)
        master = con.root.Master()
        master.delete_key(key)

    # Internal APIs

    # send data to minion based on allocation scheme
    def send_to_minion(self, minion_ports, data, key):
        # note the order. Interesting behavior
        for minion_port in minion_ports:
            try:
                con = rpyc.connect("127.0.0.1", port=minion_port)
                minion = con.root.Minion()
                minion.save(data, key)
            except ConnectionRefusedError:
                print("[Client]Unable to connect. Service Unavailable. Please retry")

                # roll back
                self.delete(key)
                return False
        return True
