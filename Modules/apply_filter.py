def run(database_name):
    global db
    from pymongo import MongoClient
    from Modules import general
    import pandas as pd, numpy as np, multiprocessing as mp
    import glob, os

    #Use Multiprocessing
    use_mp = True
    black_redo = True
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
        exit = False
        while exit != True:
            print("0.Exit program?")
            print("1.Import Filters csv to MongoDB?")
            print("2.Run only whitelist?")
            print("3.Run only Blacklist?")
            print("4.Run Whitelist & Blacklist?")
            menu = input("Select which part of the program to execute: ")
            if menu =="0" or menu == "q":
                    exit = True
            if menu =="1":
                importFilters()
            if menu =="2":
                apply_filters("w")
            if menu =="3":
                apply_filters("b")
            if menu =="4":
                apply_filters("w")
                print("finished whitelist")
                apply_filters("b")

            print("Finished running the program\n")

    def importFilters():
        collection=db[filters_collection_name]
        collection.drop()
        for file in file_list:
            command=f"mongoimport --host localhost --port 27017 --db {database_name} --collection {filters_collection_name} --type csv --file {file} --headerline"
            os.system(command)

    

    def apply_filters(filter_type):
        arg_list=[]
        if filter_type == "w":
            # Use whitelisting Series
            filter_series = white_c['path']
            # Drop the relevant collections
            connection_to_whitelist.drop()

        elif filter_type == "b":
            # Use whitelisting Series
            filter_series = black_c['path']
            # Drop the relevant collections
            if black_redo == True:
                connection_to_blacklist.drop()
                connection_to_rejected.drop()
            #make the collection that the blacklist filter will delete from
            batch=[]
            i=0
            if black_redo == True :
                print("Started the copy for the blacklist")
                copy = connection_to_whitelist.find()
                for each in copy:
                    batch.append(each)
                    i=i+1
                    if i % 500000 ==0:
                        print(i)
                        connection_to_blacklist.insert_many(batch)
                        batch=[]  
                if batch != []:
                    connection_to_blacklist.insert_many(batch)
                else:
                    print(f"Last batch empty: {i}")
                print("Finished the copy for the blacklist")

        for criteria in filter_series:
            rule = criteria
            use_agregation = False
            if criteria [-1] =="/":
                folder_match = criteria[:-1]
                criteria = f"{criteria}.*"

                query = {'$or': [
                    {'filepath': folder_match},
                    {'filepath': {'$regex': criteria}}
                    ]}
            elif "*" in criteria :
                dir_name = os.path.dirname(criteria)
                pattern = criteria.replace("*", ".*")
                query = [
                        {
                            '$match': {
                                'dir_name': dir_name, 
                                '$or': [
                                    {'filetype': 'f'},
                                    {'filetype': 'l'},
                                    {'filetype': 'lf'}
                                ]
                            }
                        }, {
                            '$match': {'filepath': {'$regex': pattern}}
                        }
                    ]            
                use_agregation = True
            else:
                query = {
                    'filepath': criteria
                }
            
            if use_mp == True:
                if filter_type == "w":
                    createFiltered(src_name,query,whitelist_name,"w",use_agregation,rule)
                elif filter_type == "b":
                    print(query)
                    arg_list.append((whitelist_name,query,rejected_name,"w",use_agregation,rule))
                    arg_list.append((whitelist_name,query,blacklist_name,"b",use_agregation,rule))
            
                if len(arg_list) >= mp_processes:
                    pool = mp.Pool(processes=len(arg_list))
                    pool.starmap(createFiltered, arg_list)
                    arg_list=[]
                    pool.close()

            elif use_mp == False:
                if filter_type == "w":
                    createFiltered(src_name,query,whitelist_name,"w",use_agregation,rule)
                elif filter_type == "b":
                    print(query)
                    createFiltered(whitelist_name,query,rejected_name,"w",use_agregation,rule)
                    createFiltered(whitelist_name,query,blacklist_name,"b",use_agregation,rule)



    main()

def createFiltered(source_collection,query,destination_collection,filter_type,use_agregation,rule):    
        batch=[]
        collection = db[source_collection]
        new_coll = db[destination_collection]
        if use_agregation == True:
            results = collection.aggregate(query)
        elif use_agregation == False and not filter_type == "b":
            results = collection.find(query)

        if filter_type == "w":
            i=0
            
            for each in results:
                each["filter_applied"] = rule
                batch.append(each)
                i=i+1
                if i % 500000 ==0:
                    print(i)
                    new_coll.insert_many(batch)
                    batch=[]  
            if batch != []:
                new_coll.insert_many(batch)
            else:
                print(f"Last batch empty: {i} insert: {rule}")
        elif filter_type == "b":
            if use_agregation == True:
                i=0
                for result in results:
                    batch.append(result['_id'])
                    i=i+1
                    if i % 500000 ==0:
                        print(i)
                        new_coll.delete_many({"_id" : {"$in": batch}})
                        batch=[]  
                if batch != []:
                    new_coll.delete_many({"_id" : {"$in": batch}})
                else:
                    print(f"Last batch empty: {i} delete: {rule}")         
            else:
                new_coll.delete_many(query)