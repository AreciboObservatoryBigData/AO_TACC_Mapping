from Modules import general
from Modules import global_vars
# importing ObjectId from bson library
from bson.objectid import ObjectId

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

def deleteByListingPathsID(table_name, listing_paths_id):
    print("Deleting from " + table_name + " where listing_paths_ID = " + str(listing_paths_id))
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[table_name]
    # delete all files from directory
    collection.delete_many({"listing_paths_ID": listing_paths_id})

def getLinksNoBroken(table_name):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[table_name]
    # Get all documents where filetype is "l" and broken? field is not found
    links = collection.find({"filetype": "l", "broken?": {"$exists": False}}, {"_id": 1, "filepath": 1, "points_to": 1})
    return links

def updateByID(table_name, ID, update_dict):
    # print("Updating " + table_name + " where _id = " + str(ID))
    db = general.connectToDB(global_vars.db_name)
    collection = db[table_name]

    collection.update_one({"_id": ObjectId(ID)}, {"$set": update_dict})
    
