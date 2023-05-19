import pymongo
def connectToDB(database_name):
    client = pymongo.MongoClient("localhost", 27017)
    db = client[database_name]
    return db