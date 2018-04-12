import rpyc
import pickle
import os
import errno
import shutil
from rpyc.utils.server import ThreadedServer

DATA_DIR = "/tmp/minion/"


class MinionService(rpyc.Service):
    class exposed_Minion(object):
        minionport = 0
        storage_path = ''

        def __init__(self):
            self.storage_path = "/tmp/minion/" + str(self.minionport) + "/"

        # save data by key
        def exposed_save(self, data, key):
            # check existing content and remove
            self.exposed_delete_key(key)
            pickle.dump([key,data], open(self.storage_path+key, "wb"))

        # get data by key
        def exposed_get_data_by_key(self, key):
            data = pickle.load(open(self.storage_path+key, "rb"))
            return data[1]

        # has key?
        def exposed_has_key(self, key):
            if os.path.exists(self.storage_path+key):
                return True
            return False

        # delete data by key
        def exposed_delete_key(self, key):
            # search directory
            if os.path.exists(self.storage_path + key):
                os.remove(self.storage_path + key)

        def exposed_get_all_keys(self):
            # return all file (key) in the storage
            all_keys=os.listdir(self.storage_path)
            return all_keys



def set_conf(minionport):
    minion = MinionService.exposed_Minion
    minion.minionport = minionport

    # create dir
    file_path= "/tmp/minion/" + str(minionport) + "/"

    if os.path.exists(file_path):
        shutil.rmtree(file_path)
    os.makedirs(file_path)


def startMinionService(minionport):
    set_conf(minionport)
    t = ThreadedServer(MinionService, port=minionport)
    t.start()
