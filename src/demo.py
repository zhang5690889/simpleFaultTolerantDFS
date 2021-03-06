import os
import random
import signal
import socket
import time
from multiprocessing import Process

import rpyc

from client import client
from conf import default_minion_ports, \
    default_proxy_port, \
    default_master_ports, \
    replication_factor
from master import startMasterService
from minion import startMinionService
from proxy import startProxyService
from utils import generate_file
from utils import removefiles


# This class exposes API for controlling all nodes
class WebServices:
    def __init__(self, minion_ports, master_ports, proxy_port):
        # Process reference
        self.minion_process_ref = []
        self.master_process_ref = []
        self.proxy_process_ref = None

        self.minion_ports = minion_ports
        self.master_ports = master_ports
        self.proxy_port = proxy_port
        # SERVICE LAYER #

    def activate_minion(self, minion_port):
        p = Process(target=startMinionService, args=(minion_port,))
        p.start()
        self.minion_process_ref.append(p)

    def activate_proxy(self, proxy_port, minion_ports, master_ports):
        p = Process(target=startProxyService, args=(proxy_port, master_ports, minion_ports,))
        p.start()
        self.proxy_process_ref = p

    def activate_master(self, master_port):
        p = Process(target=startMasterService, args=(master_port,))
        p.start()
        self.master_process_ref.append(p)

    def file_status_report(self):
        # connect to master
        con = rpyc.connect('localhost', self.proxy_port)
        proxy = con.root.Proxy()
        master_port = proxy.get_master()
        con = rpyc.connect("127.0.0.1", port=master_port)
        master = con.root.Master()

        result = list(master.get_file_status_report())
        print("File mapping status:")
        print(result)

    def start_all_services(self):

        # minion only knows minion port
        for minion_port in self.minion_ports:
            self.activate_minion(minion_port)

        # master only know master port
        for master_port in self.master_ports:
            self.activate_master(master_port)

        # start proxy
        # proxy should all ports
        self.activate_proxy(self.proxy_port, self.minion_ports, self.master_ports)

        # give 3 seconds to start all processes
        time.sleep(3)

    def kill_main_master(self):
        con = rpyc.connect('localhost', self.proxy_port)
        proxy = con.root.Proxy()
        master_port = proxy.get_master()
        con = rpyc.connect("127.0.0.1", port=master_port)
        master = con.root.Master()
        master_pid = master.get_master_PID()
        os.kill(master_pid, signal.SIGTERM)

        time.sleep(2)

    def fix_k_way_replication(self):
        con = rpyc.connect('localhost', self.proxy_port)
        proxy = con.root.Proxy()
        master_port = proxy.get_master()
        con = rpyc.connect("127.0.0.1", port=master_port)
        master = con.root.Master()
        master.fix_k_way_replication()

    def cleanup(self):
        for minion_ref in self.minion_process_ref:
            minion_ref.terminate()
        for master_ref in self.master_process_ref:
            master_ref.terminate()
        self.proxy_process_ref.terminate()

        # Process reference
        self.minion_process_ref = []
        self.master_process_ref = []
        self.proxy_process_ref = None

        time.sleep(3)

    # kill k - 1 nodes
    def kill_random_minions(self, num_minion_to_kill):
        # Randomly kill 2 nodes
        alive_nodes = list(range(0, len(self.minion_process_ref)))
        random.shuffle(alive_nodes)

        print("[Admin] Killing:" + str(num_minion_to_kill) + " minions")
        num_node_down = num_minion_to_kill
        for index in range(num_node_down):
            self.minion_process_ref[alive_nodes[index]].terminate()

        time.sleep(1)

    # This report contains minion cluster status: whether live or down
    def minion_status_report(self):
        con = rpyc.connect('localhost', self.proxy_port)
        proxy = con.root.Proxy()
        master_port = proxy.get_master()
        con = rpyc.connect("127.0.0.1", port=master_port)
        master = con.root.Master()
        master.minion_status_report()


class demo:
    def __init__(self):
        self.webservice = WebServices \
            (default_minion_ports, default_master_ports, default_proxy_port)

    # Race condition is possible.
    # If user makes a request too fast, the later request would get outdated value
    def testNameSpaceOverwrite(self, expectedResult, namespace, client_service, counter, path):

        try:
            client_service.put(path, namespace)
            result2 = client_service.get(namespace)
            assert result2 == expectedResult, "Same namespace file not overridden."
        except AssertionError:
            if counter > 10:
                raise
            else:
                time.sleep(2)
                print('Request too fast. System has not been updated. Retrying!')
                counter += 1
                self.testNameSpaceOverwrite(expectedResult, namespace, client_service, counter, path)

    # Test cases #

    # Test 1: basic DFS functionality
    #  Features tested:
    #       Client: Put, Get, Delete
    def test1(self):
        print("Test 1 running.............")
        self.webservice.start_all_services()
        client_service = client(self.webservice.proxy_port)

        # Initialize 3 files
        path1 = './test1.txt'
        path2 = './test2.txt'
        path3 = './test3.txt'

        text1 = "this is test1"
        text2 = "this is test2"
        text3 = "this is test3"

        namespace1 = 'test1'
        namespace2 = 'test2'

        # generate 3 files
        generate_file(path1, text1)
        generate_file(path2, text2)
        generate_file(path3, text3)

        # upload 3 files. file 3 should overwrite file 2
        client_service.put(path1, namespace1)
        client_service.put(path2, namespace2)

        #
        # # Get 3 files
        result1 = client_service.get(namespace1)
        assert result1 == text1, "Get or put not working! File content not same"

        self.testNameSpaceOverwrite(text3, namespace2, client_service, 0, path3)

        client_service.delete(namespace2)

        result2 = client_service.get(namespace2)
        assert result2 == "", "Delete not working"

        result1 = client_service.get(namespace1)
        assert result1 == text1, "Other file got affected after deleting a file"

        print("[Test 1 passed]. Basic client put, get, delete working!")

        # remove generated file
        removefiles([path1, path2, path3])

        self.webservice.cleanup()

    # Test 2: k way replication validation (backup fault tolerant)
    #  Precondition: User has successfully uploaded an file
    #  Steps:
    #     1. k -1 node go offline
    #     2. User retrieves the previously uploaded file
    #     3. User can still get the whole file back
    def test2(self):
        print("Test 2 running.............")
        self.webservice.start_all_services()

        # Precondition
        client_service = client(self.webservice.proxy_port)

        # Generate a file with some data
        path = './test2.txt'
        text = "test2 data"
        namespace = 'test2'
        generate_file(path, text)

        status = client_service.put(path, namespace)
        assert status is True, "Put failed"
        # End of precondition

        # Randomly kill k - 1 nodes
        self.webservice.kill_random_minions(replication_factor - 1)

        retrieved_data = client_service.get(namespace)
        # Compare stored and retrieved value
        assert text == retrieved_data, ("Data corrupted!")

        print("[Test 2 passed] k - 1 minion offline successful!")
        removefiles([path])
        self.webservice.cleanup()

    # Test 3: client to minion fail tell user
    # (warn client something wrong. Tell user to retry)
    #  Steps:
    #     1. Client sends a chunk of data to a minion based on allocation scheme
    #     2. The minion goes offline
    #     3. The client should request for new allocation scheme
    def test3(self):
        print("Test 3 running.............")
        self.webservice.start_all_services()
        client_service = client(self.webservice.proxy_port)

        # Try to connect to a non existing minion
        status = client_service.send_to_minion \
            ([7777], "abc", 'abc')

        assert status is False, "It is connecting to a none existing client"

        print("[Test 3 passed] client to minion fault handling working!")

        self.webservice.cleanup()

    # Test 4: dead minion detection (master to minion fault)
    #  Steps:
    #     1. upon put and get, master know there are some dead minions
    def test4(self):
        print("Test 4 running.............")
        self.webservice.start_all_services()

        # Randomly kill k - 1 nodes
        self.webservice.kill_random_minions(replication_factor - 1)
        self.webservice.minion_status_report()

        path1 = './test1.txt'
        text1 = "this is test1"
        namespace1 = 'test1'

        client_service = client(self.webservice.proxy_port)
        generate_file(path1, text1)
        client_service.put(path1, namespace1)
        result1 = client_service.get(namespace1)

        assert result1 == text1, "Get or put not working after killing some minions"

        print("[Test 4 passed] After killing some minions, put and get continue to work!")

        removefiles([path1])
        self.webservice.cleanup()

    # Test 5: master down (proxy to master fault)
    #  Steps:
    #     1. when the main master is down, the backup master should take over
    #     2. put and get should continue to function
    def test5(self):
        print("Test 5 running.............")
        self.webservice.start_all_services()

        # Precondition test
        client_service = client(self.webservice.proxy_port)

        # Generate a file with some data
        path = './test2.txt'
        text = "test2 data"
        dest_name = 'test2'
        generate_file(path, text)

        # perform user operations
        client_service.put(path, dest_name)

        self.webservice.kill_main_master()

        result = client_service.get(dest_name)
        assert result == text, "Get or put not working after killing main master"

        print("[Test 5 passed]: Main master down! Get and put continue to work!")
        removefiles([path])
        self.webservice.cleanup()

    # Test 6: minion k way replication fix
    # Precondition:
    # 1. When admin decides to fix k way replication, system has to be in a steady state.
    #    Otherwise replicate could face a racing condition
    # 2. A file has been uploaded
    # Steps:
    #  1. kill k - 1 minions
    #  2. show report
    #  3. run minion fix on master
    #  4. show all minion show remain k replication
    def test6(self):
        print("Test 6 running.............")
        self.webservice.start_all_services()

        # Precondition
        client_service = client(self.webservice.proxy_port)

        path2 = './test2.txt'
        text2 = "test2 data"
        dest_name2 = 'test2'
        generate_file(path2, text2)

        path1 = './test1.txt'
        text1 = "test1 data"
        dest_name1 = 'test1'
        generate_file(path1, text1)

        # upload 2 files
        client_service.put(path2, dest_name2)
        client_service.put(path1, dest_name1)

        # Randomly kill k - 1 nodes
        self.webservice.kill_random_minions(replication_factor - 1)

        # show minion status
        self.webservice.minion_status_report()

        # show file report report
        print("Before fixing.")
        self.webservice.file_status_report()

        # k replication fix
        self.webservice.fix_k_way_replication()

        print("After fixing.")
        # show show report report
        self.webservice.file_status_report()

        removefiles([path1, path2])
        self.webservice.cleanup()

    # Test 7: client to master fail (race condition handling)
    # (warn client something wrong. Tell user to retry)
    #  Steps:
    #     1. Client asks proxy for a live master
    #     2. Proxy sends a live master's port back
    #     3. Master goes offline
    #     4. Client cannot connect to master
    #     5. Client retry by calling to proxy again
    def test7(self):

        print("Test 7 running.............")
        self.webservice.start_all_services()
        client_service = client(self.webservice.proxy_port)

        # Try to connect to a non existing master. System will retry
        client_service.connect_to_master(7777)

        print("[Test 7 passed] client to master race handling working!")

        self.webservice.cleanup()



    def run_all_tests(self):
        self.test1()
        self.test2()
        self.test3()
        self.test4()
        self.test5()
        self.test6()
        self.test7()

if __name__ == "__main__":
    demo_obj = None
    try:
        demo_obj = demo()
        demo_obj.run_all_tests()

    except socket.error as e:
        print("Unexpected exception! Check logic")
        demo_obj.cleanup()
