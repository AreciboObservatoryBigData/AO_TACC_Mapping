
from Modules import general
import multiprocessing as mp
import psutil
import time
import sys

serial = False

def run(files_db_info, db_name):
    separator = "\t,;"
    
    for info in files_db_info:
        # first element is a list of files
        files = info[0]
        # second element is the collection name
        collection_name = info[1]
        
        for file in files:
            print("Processing " + file)
            # Make pool of 10X the number of cores
            pool = mp.Pool(processes=mp.cpu_count()*10)
            header = []
            start_time = time.time()
            memory_stats = psutil.virtual_memory()
            submissions = []
            i = 0
            for line in open(file, "r"):
                if line[-1] == "\n":
                    line = line[:-1]
                if i == 0:
                    header = line.split(separator)
                    i += 1
                    continue
                split_line = line.split(separator)
                dict_line = dict(zip(header, split_line))
                # convert filesize to int

                dict_line["filesize"] = int(dict_line["filesize"])

                # Convert fileAtime to double
                dict_line["fileAtime"] = float(dict_line["fileAtime"])
                # Convert fileMtime to double
                dict_line["fileMtime"] = float(dict_line["fileMtime"])
                # Convert fileCtime to double
                dict_line["fileCtime"] = float(dict_line["fileCtime"])
                submissions.append(dict_line)

                if memory_stats.percent > 90:
                    print("Memory usage is too high")
                    # Waiting for memory to be below 80%
                    while memory_stats.percent > 80:
                        time.sleep(1)
                        memory_stats = psutil.virtual_memory()
                    print("Memory usage is now below 80%")

                # Print the progress
                if i % 500000 == 0:
                    # check current used memory
                    memory_stats = psutil.virtual_memory()

                    if serial:
                        submitInserts(submissions,db_name,collection_name)
                    else:
                        pool.apply_async(submitInserts, args=(submissions,db_name,collection_name))
                    submissions = []
                    print(i)
                
                i += 1
        if serial:
            submitInserts(submissions,db_name,collection_name)
        else:
            pool.apply_async(submitInserts, args=(submissions,db_name,collection_name))
        pool.close()
        pool.join()
        end_time = time.time()
        print("Time taken: " + str(end_time - start_time))

def submitInserts(submissions,db_name, collection_name):
    db = general.connectToDB(db_name)
    collection = db[collection_name]
    collection.insert_many(submissions)


