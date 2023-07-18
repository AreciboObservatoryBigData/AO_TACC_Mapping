from pymongo import MongoClient
from Modules import general
import pandas as pd, numpy as np, multiprocessing as mp
import glob, os, time
from Modules import global_vars



def run(database_name):
    global db
    global black_redo

    #Use Multiprocessing
    use_mp = True
    black_redo = False
    mp_processes = 50

    #paths
    csv="general_files/filters/"
    file_list = glob.glob(os.path.join(csv , "*.csv"))
    # connection
    db=general.connectToDB(database_name)

    #collections
    filters_collection_name = "filters"
    src_name = "src_listing"
    whitelist_name = "src_whitelist_filtered"
    blacklist_name = "src_final_filtered"
    rejected_name = "blacklist_rejects"
    connection_to_filters = db[filters_collection_name]
    connection_to_whitelist = db["src_whitelist_filtered"]
    connection_to_blacklist = db["src_final_filtered"]
    connection_to_rejected = db["blacklist_rejects"]

    #dataframes
    dataframes = []
    for file in file_list:
        dataframe = pd.read_csv(file)
        dataframes.append(dataframe)
    if len(dataframes) == 0:
        print("No criteria csv found")
    elif len(dataframes) == 1:
        bw = dataframes[0]
    else:
        concatenated_df = pd.concat(dataframes)
        concatenated_df = concatenated_df.reset_index(drop=True)
        bw = concatenated_df

    column_to_split = 'filter_type'
    white_c = bw[bw[column_to_split] == 'w']
    black_c = bw[bw[column_to_split] == 'b']

    def main():
        global black_redo
        exit = False
        while exit != True:
            print("0.Exit program?")
            print("1.Delete filters from MongoDB?")
            print("2. Reset Blacklist: " + str(black_redo))
            print("3.Run only whitelist?")
            print("4.Run only Blacklist?")
            print("5.Run Whitelist & Blacklist?")
            menu = input("Select which part of the program to execute: ")
            if menu =="0" or menu == "q":
                    exit = True
            if menu =="1":
                connection_to_filters.drop()
            if menu =="2":
                black_redo = not black_redo
            if menu =="3":
                apply_filters("w")
            if menu =="4":
                apply_filters("b")
            if menu =="5":
                apply_filters("w")
                print("finished whitelist")
                apply_filters("b")

            print("Finished running the program\n")

    def resetCollections(collection):
        collection.drop()
        collection.create_index('filename')
        collection.create_index('dir_name')
        collection.create_index('filepath')

    def apply_filters(filter_type):
        
        if filter_type == "w":
            print("Running whitelist")
            # Use whitelisting Series
            filter_series = white_c['path']
            # Drop the relevant collections
            resetCollections(connection_to_whitelist)


            
            or_list = []
            for criteria in filter_series:
                if connection_to_filters.find_one({"path": criteria}):
                    continue
                
                
                # Get query
                query = ruleToAggregation(criteria)
                # Create a single aggregation with ors out of each of the match stages
                or_list.append(query[0]["$match"])
            # Make aggregation with the out stage
            startTime= time.time()
            aggregation = [
                {
                    "$match":{
                        "$or": or_list
                    }
                },{
                    "$out": whitelist_name
                }
            ]

            collection = db[src_name]
            collection.aggregate(aggregation)
            endTime=time.time()
            print(f"Time taken for applying whitelist: {endTime-startTime}")                     

            for criteria in filter_series:
                if connection_to_filters.find_one({"path": criteria}):
                    continue
                
                document = {
                    'path': criteria,
                    'filter_type': filter_type,
                }
                connection_to_filters.insert_one(document)

        elif filter_type == "b":
            print("Running blacklist")
            start_time = time.time()
            # Use whitelisting Series
            filter_series = black_c['path']
            # Drop the relevant collections
            if black_redo == True:
                resetCollections(connection_to_blacklist)
                resetCollections(connection_to_rejected)
                print("Started the copy for the blacklist")
                pipeline = [{ "$out": blacklist_name }]
                connection_to_whitelist.aggregate(pipeline)
                print("Finished the copy for the blacklist")
            # blacklist would be deleting from the blacklist collection and adding to the rejected collection
            args_list = []
            for criteria in filter_series:
                if connection_to_filters.find_one({"path": criteria}):
                    continue
                
                
                # Get query
                aggregation = ruleToAggregation(criteria)
                # Create a single aggregation with ors out of each of the match stages
                args_list.append((aggregation, blacklist_name))
            print("Finished creating the args list")
            print("Deleting documents...")
            if use_mp == True:
                with mp.Pool(processes=mp_processes) as pool:
                    pool.starmap(deleteFromAggregation, args_list)
            else:
                for i,args in enumerate(args_list):
                    print(i)
                    deleteFromAggregation(*args)
            # Get the difference between the whitelist and blacklist and output it to the rejected collection
            # We would be getting all in whitelist not in blacklist
            aggregation = [
                {
                    "$lookup":{
                        "from": blacklist_name,
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "results"
                    }
                },{
                    "$match":{
                        "results": []
                    }
                },{
                    "$project":{
                        "results": 0
                    }
                },{
                    "$out": rejected_name
                }
            ]
            whitelist_collection = db[whitelist_name]
            whitelist_collection.aggregate(aggregation)

            # Add the applied filters to the filters collection
            for criteria in filter_series:
                if connection_to_filters.find_one({"path": criteria}):
                    continue

                document = {
                    'path': criteria,
                    'filter_type': filter_type,
                }
                connection_to_filters.insert_one(document)


            print(f"Time taken for applying blacklist: {time.time()-start_time}")
            



    main()

def ruleToAggregation(criteria):
    if criteria[0] != "/":
    
        query = [{
                "$match": {
                    "filepath": {"$regex": criteria}
                }
            }]
    
    elif criteria [-1] =="/":
        folder_match = criteria[:-1]
        criteria = f"{criteria}.*"

        query = [
                {'$match': {'$or': [
                            {'filepath': folder_match}, 
                            {'filepath': {'$regex': criteria}}
                        ]}}                        
            ]
    elif "*" in criteria :
        dir_name = os.path.dirname(criteria)
        pattern = criteria.replace(".", "\.")
        pattern = criteria.replace("*", ".*")
        query = [
                {'$match': {'dir_name': dir_name, 
                        '$or': [
                            {'filetype': 'f'},
                            {'filetype': 'l'},
                            {'filetype': 'lf'},
                            {'filetype': 'll'}
                        ],
                        'filepath': {'$regex': pattern}
                        }}
            ]            
    else:
        query = [{'$match': {'filepath': criteria}}]
    return query


def deleteFromAggregation(aggregation, collection_name):
    database_name = global_vars.db_name
    # connection
    db=general.connectToDB(database_name)
    collection = db[collection_name]
    aggregation = aggregation + [
        {
            "$project": {
                "_id": 1
            }
        }
    ]
    results = collection.aggregate(aggregation)
    # Delete by batches
    batch = []
    for result in results:
        batch.append(result["_id"])
        if len(batch) == 500000:
            collection.delete_many({"_id": {"$in": batch}})
            batch = []
    if batch != []:
        collection.delete_many({"_id": {"$in": batch}})