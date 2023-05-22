import pymongo

from Modules import general
from Modules import global_vars

def getDirs(collection_name):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]
    # get all directories
    dirs = collection.find({"filetype": "d"}, {"_id": 1, "filepath": 1})
    return dirs

def getIDsFromDir(dir_info,collection_name):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]
    # get all files from directory
    files = collection.find({"filepath": {"$regex": "^" + dir_info["filepath"] + "/"}}, {"_id": 1})
    return files

# def insertFileDir(listing_table_name, file_dir_table_name):
#     aggregation = [
#         {"$match": {"filetype": "d"}},
#         {"$project": {"_id": 1, "filepath": 1}},
#         {"$lookup": {
#             "from": listing_table_name,

#         }}
#     ]