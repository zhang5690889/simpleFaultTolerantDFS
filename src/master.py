import random
import os
import rpyc
from rpyc.utils.server import ThreadedServer

from conf import replication_factor


def set_conf():
    master = MasterService.exposed_Master
    master.replication_factor = replication_factor


class MasterService(rpyc.Service):
    class exposed_Master(object):
        minion_ports = []
        replication_factor = 0

        def exposed_get_allocation_scheme(self):
            live_nodes = self.get_minion_state()[0]
            assert len(live_nodes)>=replication_factor, "live nodes less than replication factor"

            # randomly choose 2 elements
            ports = random.sample(live_nodes, replication_factor)
            return ports

        def exposed_get_minion_that_has_the_key(self, key):

            #     ask all minion who has the key
            minions_who_have_the_key = []

            for minion_port in self.minion_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=minion_port)
                    minion = con.root.Minion()
                    if minion.has_key(key):
                        minions_who_have_the_key.append(minion_port)
                except Exception:
                    continue

            # TODO:    k way replication fix. Keeps a counter
            return minions_who_have_the_key


        # master tells pid for others to kill
        def exposed_get_master_PID(self):
            return os.getpid()

        def exposed_delete_key(self, key):

            # notify all minions. If you have this key, delete it
            for minion_port in self.minion_ports:
                con = rpyc.connect("127.0.0.1", port=minion_port)
                minion = con.root.Minion()
                minion.delete_key(key)


        def exposed_get_file_status_report(self):

            all_mapping = []
            # broadcast to get key value
            for minion_port in self.minion_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=minion_port)
                    minion = con.root.Minion()
                    # copy to a new place
                    tmp = list(minion.get_all_keys())
                    all_mapping.append([minion_port,tmp])
                except Exception:
                    continue

            print (all_mapping)




        # This method shows how many minion down
        def exposed_minion_status_report(self):
            minion_state=self.get_minion_state()

            print("The following minions are dead:")
            print(minion_state[1])
            print("The following minions are alive:")
            print(minion_state[0])
            if len(minion_state[1])< self.replication_factor+1:
                print ("The system is in danger! Make sure you have more live nodes.")

        def exposed_get_minion_ports(self,minion_ports):
            # minior bug. Dirty fix
            self.minion_ports.clear()
            for minion_port in minion_ports:
                self.minion_ports.append(minion_port)
            # print (self.minion_ports)


        # returns live nodes and dead nodes
        def get_minion_state(self):
            deadminions = []
            for minion_port in self.minion_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=minion_port)
                    con.root.Minion()
                except ConnectionRefusedError:
                    deadminions.append(minion_port)
                    continue
            live_nodes = set(self.minion_ports) - set(deadminions)
            return [list(live_nodes),deadminions]


def startMasterService(master_port):
    set_conf()
    t = ThreadedServer(MasterService, port=master_port)
    t.start()
