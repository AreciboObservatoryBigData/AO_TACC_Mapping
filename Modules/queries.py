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

    collection.update_one({"_id": ID}, {"$set": update_dict})

def getLinksNotBrokenNoPointsID(table_name):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[table_name]
    # Get all documents where filetype is "l" and broken? field is not found
    links = collection.find({"filetype": "l", "broken?": 0, "points_to_ID": {"$exists": False}}, {"_id": 1, "filepath": 1, "points_to": 1})
    return links  
def getElementIDFromFilepath(table_name, filepath):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[table_name]
    # Get all documents where filetype is "l" and broken? field is not found
    element = collection.find_one({"filepath": filepath}, {"_id": 1})
    return element

def getAInBByFilepath(src_base_path, dst_base_path, src_name, dst_name, eq_value = 1):

    # connect to database
    db = general.connectToDB(global_vars.db_name)
    src_collection = db[src_name]
    if src_base_path[-1] == "/":
        src_base_path = src_base_path[:-1]
    if dst_base_path[-1] == "/":
        dst_base_path = dst_base_path[:-1]
    aggregation = [
        {
            '$addFields': {
                'no_base': {
                    '$replaceOne': {
                        'input': '$filepath', 
                        'find': src_base_path, 
                        'replacement': ''
                    }
                }
            }
        }, {
            '$addFields': {
                'lookup_path': {
                    '$concat': [
                        dst_base_path, '$no_base'
                    ]
                }
            }
        }, {
            '$lookup': {
                'from': dst_name, 
                'localField': 'lookup_path', 
                'foreignField': 'filepath', 
                'as': 'result'
            }
        }, {
            '$addFields': {
                'results_size': {
                    '$size': '$result'
                }
            }
        }, {
            '$match': {
                'results_size': {
                    '$eq': eq_value
                }
            }
        }
    ]
    breakpoint()
    src_in_dst = src_collection.aggregate(aggregation)
    # Turn cursor into list
    src_in_dst = list(src_in_dst)
    return src_in_dst

    
def insertANotinBByFilenameFiltered(regex, A_collection_name, B_collection_name, out_collection_name, filetypes = ["f", "l", "lf", "ll"]):
    
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[A_collection_name]
    aggregation = [{
        "$match":{
            "$and":[
                {"filepath": {"$regex": regex}},
                {"filetype" : {"$in": filetypes}}
            ]
        }
    },
    { 
        "$group": { 
            "_id": "$filename", 
            "group_results":{
                "$push":{
                    "_id": "$_id",
                    "filename": "$filename",
                    "filepath": "$filepath",
                    "filetype": "$filetype",
                    "filesize": "$filesize", 
                    "dir_name": "$dir_name",
                }}
         } },
    {
        "$lookup": {
        "from": B_collection_name,
        "localField": "_id",
        "foreignField": "filename",
        "as": "lookupResult"
        }
    },
    {
        "$match":{"lookupResult": {"$eq": []}}
    }, {
        '$project': {
            '_id': 0, 
            'lookupResult': 0
        }
    }, {
        '$unwind': {
            'path': '$group_results'
        }
    }, {
        '$project': {
            '_id': '$group_results._id', 
            'filename': '$group_results.filename', 
            'filepath': '$group_results.filepath', 
            'filetype': '$group_results.filetype', 
            'filesize': '$group_results.filesize', 
            'dir_name': '$group_results.dir_name'
        }
    },{"$out": out_collection_name}

    ]

    collection.aggregate(aggregation)



def getDistinctValues(collection_name, field_name):
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]

    aggregation = [
        {
            "$group":{
                "_id": f"${field_name}"
            }
        },{
            "$project":{
                "_id": 0,
                field_name: "$_id"
            }
        }
    ]
    results = collection.aggregate(aggregation)

    return results





def getAinBByFilename(filename, listing_collection_name, return_all = True, filetypes = ["f"]):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[listing_collection_name]
    # Get all documents where filepath starts with base_path
    if return_all:
        documents = collection.find({"filename": filename, "filetype" : {"$in": filetypes}})
        documents = list(documents)
    else:
        documents = collection.find_one({"filename": filename, "filetype" : {"$in": filetypes}})
        documents = [documents]
    return documents

def getRegexMatches(collection_name, regex, case_sensitive = False, limit = 20):
    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]
    if case_sensitive:
        find_dict = {"filepath": {"$regex": regex}}
    else:
        find_dict = {"filepath": {"$regex": regex, "$options": "i"}}
    # Get all documents where filepath starts with base_path
    documents = collection.find(find_dict).limit(limit)
    return documents

def getAnalysisByDistinctDirectories(collection_name, levels_num, match_stage = {}):
    # Create db connection
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]
    aggregation = [
        {
            "$match": match_stage

        },
        {
            '$addFields': {
                'split_path': {
                    '$split': [
                        '$filepath', '/'
                    ]
                }
            }
        }, {
            '$addFields': {
                'sliced_string': {
                    '$slice': [
                        '$split_path', 0, levels_num
                    ]
                }
            }
        }, {
            '$group': {
                '_id': 'sliced_string', 
                'distinctValues': {
                    '$addToSet': '$sliced_string'
                }
            }
        }
    ]

    results = collection.aggregate(aggregation)
    return results

def getDistinctDirNameByRegex(collection_name, regex, case_sensitive = False):

    # connect to database
    db = general.connectToDB(global_vars.db_name)
    collection = db[collection_name]
    if case_sensitive:
        find_dict = {"filepath": {"$regex": regex}}
    else:
        find_dict = {"filepath": {"$regex": regex, "$options": "i"}}
    # Get all documents where filepath starts with base_path
    aggregation = [
        {
            "$match": find_dict
        },
        {
            "$group":{
                "_id": "$dir_name"
            }
        }
    ]
    results = collection.aggregate(aggregation)
    dir_names = []
    for result in results:
        dir_names.append(result["_id"])
    return dir_names



