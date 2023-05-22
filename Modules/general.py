import pymongo
import glob
import os
def connectToDB(database_name):
    client = pymongo.MongoClient("localhost", 27017)
    db = client[database_name]
    return db
def getRecursiveFiles(dir_path):
    # Get first directories and files
    listing = glob.glob(os.path.join(dir_path, "*"))
    dirs = []
    files = []
    for item in listing:
        if os.path.isdir(item):
            dirs.append(item)
        elif os.path.isfile(item):
            files.append(item)
        else:
            print("ERROR! with item: " + item)
    while dirs != []:
        dir_path = dirs[0]
        dirs.pop(0)
        listing = glob.glob(os.path.join(dir_path, "*"))
        dirs = []
        files = []
        for item in listing:
            if os.path.isdir(item):
                dirs.append(item)
            elif os.path.isfile(item):
                files.append(item)
            else:
                print("ERROR! with item: " + item)
    return files