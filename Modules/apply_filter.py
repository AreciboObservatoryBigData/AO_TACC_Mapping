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
    whitelist_collection_name = "whitelist"
    blacklist_collection_name = "blacklist"
    src_name = "src_listing"
    whitelist_name = "src_whitelist_filtered"
    connection_to_whitelist = db["src_whitelist_filtered"]
    connection_to_blacklist = db["src_final_filtered"]

    #dataframes
    dataframes = []
    for file in file_list:
        dataframe = pd.read_csv(file)
        dataframes.append(dataframe)
    concatenated_df = pd.concat(dataframes)
    concatenated_df = concatenated_df.reset_index(drop=True)
    bw = concatenated_df
    column_to_split = 'filter_type'
    white_c = bw[bw[column_to_split] == 'w']
    black_c = bw[bw[column_to_split] == 'b']

    def main():
        exit = False
        while exit != True:
            print("0.Exit program")
            print("1.Import Whitelist csv to MongoDB?")
            print("2.Import Blacklist csv to MongoDB?")
            print("3.Run only whitelist?")
            print("4.Run only Blacklist?")
            print("5.Run Whitelist & Blacklist?")
            

            menu = input("Select which part of the program to execute: ")
            if menu =="0" or menu == "q":
                exit = True
            if menu =="1":
                importWhitelist()
            if menu =="2":
                importBlacklist()
            if menu =="3":
                whitelisting()
            if menu =="4":
                blacklisting()
            if menu =="5":
                whitelisting()
                blacklisting()
            
            print("Finished running the program\n\n")

    def importWhitelist():
        collection=db[whitelist_collection_name]
        collection.drop()
        data = white_c.to_dict(orient='records')
        collection.insert_many(data)

    def importBlacklist():
        collection=db[blacklist_collection_name]
        collection.drop()
        data = black_c.to_dict(orient='records')
        collection.insert_many(data)

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
            createFiltered(collection,query,connection_to_blacklist,1)



    main()