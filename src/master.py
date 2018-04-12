import os
import random

import rpyc
from rpyc.utils.server import ThreadedServer

from conf import replication_factor


# master server config
def set_conf():
    master = MasterService.exposed_Master
    master.replication_factor = replication_factor


class MasterService(rpyc.Service):
    class exposed_Master(object):
        minion_ports = []
        replication_factor = 0

        # tell how to allocation resources
        def exposed_get_allocation_scheme(self):
            live_nodes = self.get_minion_state()[0]
            assert len(live_nodes) >= replication_factor, "live nodes less than replication factor"

            # randomly choose 2 elements
            ports = random.sample(live_nodes, replication_factor)
            return ports

        # get a list of minion how has key
        def exposed_get_minion_that_has_the_key(self, key):

            # ask all minion who has the key
            minions_who_have_the_key = []
            for minion_port in self.minion_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=minion_port)
                    minion = con.root.Minion()
                    if minion.has_key(key):
                        minions_who_have_the_key.append(minion_port)
                except Exception:
                    continue
            return minions_who_have_the_key

        # master tells pid for others to kill
        def exposed_get_master_PID(self):
            return os.getpid()

        # delete a key from minion
        def exposed_delete_key(self, key):

            # notify all minions. If you have this key, delete it
            for minion_port in self.minion_ports:
                con = rpyc.connect("127.0.0.1", port=minion_port)
                minion = con.root.Minion()
                minion.delete_key(key)

        # file status of each minion
        def exposed_get_file_status_report(self):
            all_minion_meta = []
            # broadcast to get key value
            for minion_port in self.minion_ports:
                try:
                    con = rpyc.connect("127.0.0.1", port=minion_port)
                    minion = con.root.Minion()
                    # copy to a new place. Otherwise rpyc not happy
                    tmp = list(minion.get_all_keys())
                    all_minion_meta.append([minion_port, tmp])
                except Exception:
                    continue
            return all_minion_meta

        # fix k way replication property
        def exposed_fix_k_way_replication(self):

            # get the report
            all_minion_meta = self.exposed_get_file_status_report()

            assert len(all_minion_meta) >= self.replication_factor, "Not enough live minion to do replication"

            # actual files stored
            unique_keys = self.get_all_keys_from_mapping(all_minion_meta)

            for key in unique_keys:
                minions_with_key, minions_without_key = self.minion_with_and_without_key(key, all_minion_meta)

                # key has enough copy. Do nothing
                if len(minions_with_key) >= self.replication_factor:
                    continue
                # not have enough copy
                else:
                    # select a list of minion that doesn't key
                    count_of_copys_needed = replication_factor - len(minions_with_key)
                    minins_need_copy = random.sample(minions_without_key, count_of_copys_needed)

                    # connect to a minion that has key. Command that minion to send its copy to other minions
                    con = rpyc.connect("127.0.0.1", port=minions_with_key[0])
                    minion = con.root.Minion()
                    minion.send_key_to_other_minions(key, minins_need_copy)

            print("[Master] K replication fixing done!")

        # given a key, return a list of minion who has the key and who doesnt
        def minion_with_and_without_key(self, key, all_minion_meta):
            all_minion_ports = [minion_meta[0] for minion_meta in all_minion_meta]
            minions_who_has_the_key = []
            for minion_port, keys in all_minion_meta:
                if key in keys:
                    minions_who_has_the_key.append(minion_port)
            minions_who_dont_have_the_key = list(set(all_minion_ports) - set(minions_who_has_the_key))
            return minions_who_has_the_key, minions_who_dont_have_the_key

        # returns a list of unique keys stored across all minions
        def get_all_keys_from_mapping(self, all_minion_meta):
            all_keys_with_dup = [minion_meta[1] for minion_meta in all_minion_meta]
            flat_list = [item for sublist in all_keys_with_dup for item in sublist]
            unique_keys = list(set(flat_list))
            return unique_keys

        # This method shows how many minion are live and down
        def exposed_minion_status_report(self):
            minion_state = self.get_minion_state()

            print("[Master] The following minions are dead:")
            print(minion_state[1])
            print("[Master] The following minions are alive:")
            print(minion_state[0])
            if len(minion_state[0]) <= self.replication_factor:
                print("The system is in danger! Make sure you have more live nodes. k = num of live nodes")

        # get
        def exposed_set_minion_ports(self, minion_ports):
            # minior bug. Dirty fix
            self.minion_ports.clear()
            for minion_port in minion_ports:
                self.minion_ports.append(minion_port)

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
            return [list(live_nodes), deadminions]


def startMasterService(master_port):
    set_conf()
    t = ThreadedServer(MasterService, port=master_port)
    t.start()
