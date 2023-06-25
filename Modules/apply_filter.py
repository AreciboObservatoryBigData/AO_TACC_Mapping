def run(database_name):
    from pymongo import MongoClient
    from Modules import general
    import pandas as pd, numpy as np
    import glob, os

    #paths
    csv="general_files/filters/"
    file_list = glob.glob(os.path.join(csv , "*.csv"))
    # connection
    db=general.connectToDB(database_name)

    #collections
    filters_collection_name = "filters"
    src_name = "src_listing"
    whitelist_name = "src_whitelist_filtered"
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
                whitelisting()
            if menu =="3":
                blacklisting()
            if menu =="4":
                whitelisting()
                blacklisting()

            print("Finished running the program\n\n")

    def importFilters():
        collection=db[filters_collection_name]
        collection.drop()
        for file in file_list:
            command=f"mongoimport --host localhost --port 27017 --db {database_name} --collection {filters_collection_name} --type csv --file {file} --headerline"
            os.system(command)

    def createFiltered(collection,query,new_coll,x):    
        batch=[]
        i=0
        if x == 0:
            result = collection.find(query)
            for each in result:
                batch.append(each)
                i=i+1
                if i % 250000 ==0:
                    print(i)
                    new_coll.insert_many(batch)
                    batch=[]  
            # print(i)
            if batch != []:
                new_coll.insert_many(batch)
            else:
                print(f"Last batch empty: {i}")
        else:
            new_coll.delete_many(query)

    def whitelisting():
        connection_to_whitelist.drop()
        for criteria in white_c['path']:
            if pd.isnull(criteria) or isinstance(criteria, np.float64):
                continue
            collection = db[src_name]
            if criteria [-1] =="/":
                criteria = criteria[:-1]
                criteria = f"{criteria}.*"
                query = {
                    'filepath': {
                        '$regex': criteria
                    }
                }
            elif "*" in criteria :
                criteria = criteria.replace('.', '\\.')
                criteria = criteria.replace('*', '(?:(?!/)[^/])+')
                criteria = f"{criteria}$"
                collection = db[src_name]
                query = {
                    'filepath': {"$regex": criteria}, "filetype":"f"
                }
            else:
                query = {
                    'filepath': criteria
                }
            
            # print(query)
            createFiltered(collection,query,connection_to_whitelist,0)

    def blacklisting():
        connection_to_blacklist.drop()
        connection_to_rejected.drop()
        copy = connection_to_whitelist.find()
        connection_to_blacklist.insert_many(copy)
        for criteria in black_c['path']:
            if pd.isnull(criteria) or isinstance(criteria, np.float64):
                continue
            collection = db[whitelist_name]
            if criteria [-1] =="/":
                criteria = criteria[:-1]
                criteria = f"{criteria}.*"
                query = {
                    'filepath': 
                                {
                        '$regex': criteria
                    }
                }
            elif "*" in criteria:
                criteria = criteria.replace('.', '\\.')
                criteria = criteria.replace('*', '(?:(?!/)[^/])+')
                criteria = f"{criteria}$"
                collection = db[whitelist_name]
                query = {
                    'filepath': {"$regex": criteria}, "filetype":"f"
                }
            else:
                query = {
                    'filepath': criteria
                }
            # print(query)
            createFiltered(collection,query,connection_to_rejected,0)
            createFiltered(collection,query,connection_to_blacklist,1)




    main()