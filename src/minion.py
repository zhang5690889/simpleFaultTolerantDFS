import rpyc
import pickle
import os
import errno
from filelock import Timeout, FileLock
from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"


class MinionService(rpyc.Service):
    class exposed_Minion(object):
        minionport = 0
        storage_path = ''

        def __init__(self):
            self.storage_path = "/tmp/minion/" + str(self.minionport) + "/"

        def exposed_save(self, data, key):

            # check existing content and remove
            self.exposed_delete_key(key)
            pickle.dump([key,data], open(self.storage_path+key, "wb"))



        def exposed_get_data_by_key(self, key):
            data = pickle.load(open(self.storage_path+key, "rb"))
            return data[1]


        def exposed_has_key(self, key):
            if os.path.exists(self.storage_path+key):
                return True
            return False


        def exposed_delete_key(self, key):
            # search directory
            if os.path.exists(self.storage_path + key):
                os.remove(self.storage_path + key)




def set_conf(minionport):
    minion = MinionService.exposed_Minion
    minion.minionport = minionport

    # create dir
    file_path= "/tmp/minion/" + str(minionport) + "/"
    if not os.path.exists(file_path):
        try:
            os.makedirs(file_path)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def startMinionService(minionport):
    set_conf(minionport)
    t = ThreadedServer(MinionService, port=minionport)
    t.start()
