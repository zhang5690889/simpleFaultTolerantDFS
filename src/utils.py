import os

#  generate a file given path and data
def generate_file(path, data):
    f = open(path, "w+")
    f.write(data)
    f.close()
    return path

def removefiles(paths):
    for path in paths:
        os.remove(path)

