def run(database_name):
    from pymongo import MongoClient
    from Modules import general
    import multiprocessing as mp

    # connection
    db=general.connectToDB(database_name)

    #collections
    connection_to_src_filter = db["src_final_filtered"]
    src_in_dst_mapping = db["src_in_dst_mapping"]
    dst_in_src_mapping = db["dst_in_src_mapping"]

    def main():
        print("Exit")
        print("1.Search for src in dst & src not in dst")
        print("2.Search for dst in src")
        menu=input("Which part of the program to use: ")
        
        if menu =="0" or "q":
            return
        
        elif menu == "1":
            src_in_dst_mapping.drop()
            collection = src_in_dst_mapping
            original_id = "src_id"
            collection_to_search_in = 'dst_listing'

        elif menu == "2":
            dst_in_src_mapping.drop()
            collection = dst_in_src_mapping
            original_id = "dst_id"
            collection_to_search_in = 'src_final_filtered'

        results = createFileList(collection_to_search_in,original_id)
        i=0
        documents=[]
        for result in results:
            documents.append(result)
            i=i+1
            if i % 500000 ==0:
                print(i)
                collection.insert_many(documents)
                documents=[]  
        if documents != []:
            collection.insert_many(documents)
        else:
            print(f"Last batch empty: {i}")

    def createFileList(collection_to_search_in,original_id):
        aggregation = [
            {'$match': {
                '$or': [
                    {'filetype': 'f'}, 
                    {'filetype': 'l'}, 
                    {'filetype': 'lf'}, 
                    {'filetype': 'll'}
                ]}}, 
            {'$lookup': {
                'from': collection_to_search_in, 
                'localField': 'filename', 
                'foreignField': 'filename', 
                'as': 'filename_result'
                }}, 
            {'$unwind': {
                'path': '$filename_result'
                }}, 
            {'$addFields': {
                original_id : '$_id'
                }}, 
            {'$project': {
                '_id': 0
                }}
        ]

        results = connection_to_src_filter.aggregate(aggregation)
        return results



    main()