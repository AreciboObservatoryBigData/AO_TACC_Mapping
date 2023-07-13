
# Software to import data from a csv or txt file into a mySQL table and compare the tables to check for missing files.
# Emanuel Rodriguez
# 2023-03-23
# Arecibo Observatory Big Data  

# TODO:



import os
import glob
from Modules import queries
from Modules import menus
from Modules import make_blacklist
from Modules import global_vars
from Modules import general
from Modules import import_data
from Modules import apply_filter as m_apply_filter
from Modules import mapping as src_dst_mapping
import shutil
import subprocess
import time
import pandas as pd
import multiprocessing as mp
from datetime import datetime
# importing ObjectId from bson library
from bson.objectid import ObjectId
import sys
import random

dir_listing_path = 'dir_listing/'

listings_path = 'file_listings/'
general_files_path = 'general_files/'
modules_path = 'Modules/'
backup_dir_path = "/share_skittles/db_backup"
max_backup_size_GB = 500

link_info_path = os.path.join(general_files_path, 'link_info.tsv')

destination_dir_path = os.path.join(listings_path, 'Destination_Listing/')
source_dir_path = os.path.join(listings_path, 'Source_Listing/')

database_name = "Skittles_DB"
global_vars.db_name = database_name




#Setup vaiables
################
table_names = {
    "src_listing": "src_listing",
    "src_file_dir": "src_file_dir_relations",
    "dst_listing": "dst_listing",
    "dst_file_dir": "dst_file_dir_relations",
    "listing_paths": "listing_paths",
    "missing_listing_dirs": "missing_listing_dirs",
    "src_in_dst": "src_in_dst",
    "dst_in_src": "dst_in_src",
    "src_not_in_dst": "src_not_in_dst",
    "dst_not_in_src": "dst_not_in_src",

    # apply_filters_dbs
    "filters": "filters",
    "src_whitelist_filtered": "src_whitelist_filtered",
    "src_final_filtered": "src_final_filtered"

}

indexes = {
    table_names["src_listing"]: ["filepath", "filename", "points_to", "filetype"],
    table_names["dst_listing"]: ["filepath", "filename", "points_to", "filetype"],
    table_names["src_file_dir"]: ["dir_ID", "file_ID", [[("dir_ID", 1),("file_ID", 1)], {"unique": True}]],
    table_names["dst_file_dir"]: ["dir_ID", "file_ID", [[("dir_ID", 1),("file_ID", 1)], {"unique": True}]],

}


global_vars.table_names = table_names
################

def main():

    run_dict = {
        "options": [
            "Quit",
            "Change Current Working DB",
            "Reset DB",
            "Reset Collection",
            "Import New Data",
            "Insert File Dir Relations",
            "Resolve links to ID",
            "Apply Filter",
            "Make Mapping",
            "Insert Missing Listing Dirs",
            "Search Regex",
            "Comparisons",
            "Analysis",
            "Remove Listing Entries",
            "Reset Specific Collection",
            "Make DB Backup",
            "Restore from Backup",
            "Run Tests"

            
        ],
        "functions": [
            quit,
            changeDB,
            runResets,
            resetCollection,
            importNewData,
            insertFileDir,
            resolveLinksToID,
            apply_filter,
            make_mapping,
            insertMissingListingDirs,
            searchRegex,
            comparisonMenu, 
            analysisMenu,
            deleteListingEntries,
            resetSpecificCollection,  
                     
            backupDB,
            None,
            runTests



            
        ]
            
    }

    while True:
        print("-----------Main Menu-----------")
        option = menus.get_option_main(run_dict["options"])
        run_dict["functions"][option]()

def changeDB():
    global database_name
    print("Write the name of the DB to work on: ")
    db_name = input()
    database_name = db_name
    global_vars.db_name = db_name
    print(f"Changed DB to {database_name}")

def runResets():
    global indexes
    # connect to mongoDB, drop all collections
    # connect to DB
    db = general.connectToDB(database_name)
    print("Dropping all collections")
    # drop all collections
    for collection in db.list_collection_names():
        db[collection].drop()
    
    print("Re-establishing indexes")
    for table in table_names.values():
        if table not in indexes.keys():
            continue
        collection = db[table]

        for index in indexes[table]:
            if type(index) == str:
                collection.create_index(index)
            else:
                collection.create_index(index[0], **index[1])

def resetCollection():
    # Choose collection to reset
    print("Choose collection to reset: ")
    options = list(table_names.values())
    option = menus.get_option_main(options)
    collection_name = options[option]
    # connect to mongoDB, drop collection
    # connect to DB
    db = general.connectToDB(database_name)
    print(f"Dropping collection {collection_name}")
    # drop collection
    db[collection_name].drop()

    # Re-establish index
    if collection_name in indexes.keys():
        collection = db[collection_name]
        for index in indexes[collection_name]:
            if type(index) == str:
                collection.create_index(index)
            else:
                collection.create_index(index[0], **index[1])




def importNewData():

    # run for dst for now

    # Get all files in destination listing
    files = glob.glob(os.path.join(destination_dir_path, '*.txt'))
    # get all documents in listing_paths
    db = general.connectToDB(database_name)
    collection = db[table_names["listing_paths"]]
    listing_paths = collection.find({}, {"filepath": 1})
    listing_paths = [listing_path["filepath"] for listing_path in listing_paths]

    # filter out files already in listing_paths
    files = [file for file in files if file not in listing_paths]
  
    # Assign all files to dst_listing
    file_db_info = [[files, table_names["dst_listing"]]]


    # Do the same for src_listing
    files = glob.glob(os.path.join(source_dir_path, '*.txt'))
    # filter out files already in listing_paths
    files = [file for file in files if file not in listing_paths]
    
    file_db_info.append([files, table_names["src_listing"]])
    import_data.run(file_db_info, database_name, table_names["listing_paths"])


def insertFileDirFromDir(listing_dir, listing_table_name, file_dir_table_name):
    # print(listing_dir)
    # connect to DB
    db = general.connectToDB(database_name)
    # get collection
    collection = db[file_dir_table_name]

    files = queries.getIDsFromDir(listing_dir, listing_table_name)
    # make list of dicts to insert
    file_dir_relations = [{"file_ID": file["_id"], "dir_ID": listing_dir["_id"]} for file in files]

    if file_dir_relations == []:
        print(f"No files or Directories in {listing_dir}")
        return

    # insert using insert_many
    collection.insert_many(file_dir_relations) 

def insertFileDir():
    print("Inserting file dir relations")
    start_time = time.time()

    run_list = [
        (table_names["dst_listing"], table_names["dst_file_dir"])
    ]
    
    for listing_table_name, file_dir_table_name in run_list:
        # Get all documents with filetype = "d" that are not found in the dir_ID field of the file_dir_relations table
        listing_dirs = queries.getDirs(listing_table_name)
        arguments = []
        for listing_dir in listing_dirs:
            arguments.append((listing_dir,listing_table_name,file_dir_table_name))
        submitInParallel(insertFileDirFromDir, arguments)
        print(f"Finished inserting file dir relations in {time.time() - start_time} seconds")
        # for argument in arguments:
        #     insertFileDirFromDir(argument[0], argument[1], argument[2])


    #############################
    # Do it using aggregations
    # queries.insertFileDir(table_names["dst_listing"], table_names["dst_file_dir"])

def identifyBrokenLinks():
    links = queries.getLinksNoBroken(table_names["src_listing"])
    # open link_info file as write
    f = open(link_info_path, "w")
    # write header
    line = "ID\tpoints_to\tbroken_link\n"
    f.write(line)
    link_i = 0
    for link in links:
        line = ""
        line += str(link["_id"]) + "\t"
        line += link["points_to"] + "\t"
        line += "\n"
        f.write(line)
        link_i += 1
    f.close()
    if link_i == 0:
        print("No links to check")
        return
    # Check if the link is broken
    command = f"ssh -J remote.naic.edu -t gpuserv5 'cd {os.path.abspath(modules_path)};python3.7 check_links.py;'"
    print("Checking links on gpuserv5")
    
    subprocess.run(command, shell=True)
    print("Adding \"broken?\" links info to DB")
    # Read new link_info file
    new_link_info_path = os.path.join(os.path.dirname(link_info_path), f"new_{os.path.basename(link_info_path)}")
    f = open(new_link_info_path, "r")
    i = 0
    args = []
    for line in f:
        if i == 0:
            i += 1
            continue
        if line[-1] == "\n":
            line = line[:-1]
        split_line = line.split("\t")
        ID = split_line[0]
        broken_link = int(split_line[-1])
        update_dict = {
            "broken?": broken_link
        }
        args.append((table_names["src_listing"],ObjectId(ID), update_dict))

            
        i += 1
    f.close()

    
    submitInParallel(queries.updateByID, args, get_results=False, check_progress=False)
    # for arg in args:
    #     breakpoint()
    #     queries.updateByID(arg[0], arg[1], arg[2])
    print(f"Updated {i-1} links")
    # delete both link_info files
    # os.remove(link_info_path)
    # os.remove(new_link_info_path)

def resolveLinksToID():
    print("Resolving links to ID")
    print("Getting links to resolve")
    # Get all links in src_listing that are not broken and do not already have points_to_ID
    links = queries.getLinksNotBrokenNoPointsID(table_names["src_listing"])
    
    
    
    # for each link, get the ID of the points_to
    arguments = []
    links_info = []
    print("Getting point_IDs of links")
    for link in links:
        links_info.append(link)
        arguments.append((table_names["src_listing"], link["points_to"]))
    
    IDs = submitInParallel(queries.getElementIDFromFilepath, arguments)
    print("Updating links")

    arguments = []
    for i, result in enumerate(IDs):
        if result is None:
            continue
        ID = links_info[i]["_id"]
        update_dict = {
            "points_to_ID": result["_id"]
        }
        arguments.append((table_names["src_listing"], ID, update_dict))
    submitInParallel(queries.updateByID, arguments)
    print("Finished updating links")

def apply_filter():

    # Run filter with Database_name
    m_apply_filter.run(database_name)

def make_mapping():
    src_dst_mapping.run(database_name)

def insertMissingListingDirs():
    print("RESET missing_listing_dirs?")
    options = ["No", "Yes"]
    option = menus.get_option_main(options)
    option = options[option]
    if option == "Yes":
        reset = True
    else:
        reset = False

    db = general.connectToDB(database_name)
    if reset:
        print("Resetting missing_listing_dirs")
        collection = db[table_names["missing_listing_dirs"]]
        collection.drop()
    print("Getting links to insert")
    # get all links that are not broken and do not have points_to_ID
    links = queries.getLinksNotBrokenNoPointsID(table_names["src_listing"])  

    # make insert list
    insert_list = []
    print("Making insert list")
    for link in links:
        # check if the ID is in the missing_listing_dirs table
        collection = db[table_names["missing_listing_dirs"]]
        result = collection.find_one({"_id": link["_id"]})
        if result is None:
            insert_list.append(link)

    
    # insert into listing_dirs
    collection = db[table_names["missing_listing_dirs"]]
    print("Inserting into missing_listing_dirs")
    if len(insert_list) > 0:
        collection.insert_many(insert_list)




def resetSpecificCollection():
    print("Select which collection to reset")
    options = [table_names[key] for key in table_names.keys()]

    option = menus.get_option_main(options)
    collection_name = options[option]

    print(f"Resetting {collection_name}")
    db = general.connectToDB(database_name)
    collection = db[collection_name]
    collection.drop()
    print(f"Finished {collection_name}") 

def searchRegex():
    case_sensitive = True
    print("Please select a collection to search")

    options = [
        table_names["src_listing"],
        table_names["dst_listing"],
        table_names["src_in_dst"],
        table_names["dst_in_src"],
        table_names["src_not_in_dst"],
        table_names["dst_not_in_src"],
        table_names["src_whitelist_filtered"],
        table_names["src_final_filtered"]
    ]
    option = menus.get_option_main(options)
    collection_name = options[option]
    regex = input("Enter regex to search for:\n")
    done = False
    output_dir_path = ""
    while not done:
        print("Menu:")
        options = [
            "Exit",
            "Set collection from: " + str(collection_name),
            "Set regex from: " + str(regex),
            "Set output_dir from: " + str(output_dir_path),
            "Switch case sensitivity from: " + str(case_sensitive),
            "Seach all regex matches",
            "Search Top 20 regex matches",
            "Search distinct parent directories of regex matches",
            "Search distinct directories of regex matches",
            "Search all regex matches and export to file",
            "Search distinct parent directories of regex matches and export to file",
            "Search regex by list and export to file",
            "Search distinct parent directories by regex list and export to file"
        ]
        option = menus.get_option_main(options)
        if option == 0:
            done = True
        elif option == 1:
            collection_name = input("Enter collection name:\n")
        elif option == 2:
            regex = input("Enter regex to search for:\n")
        elif option == 3:
            output_dir_path = input("Enter output directory:\n")
        elif option == 4:
            case_sensitive = not case_sensitive
        elif option == 5:
            results = queries.getRegexMatches(collection_name, regex, case_sensitive, limit = 9999999)
            for result in results:
                print(result["filepath"])
        elif option == 6:
            results = queries.getRegexMatches(collection_name, regex, case_sensitive, limit = 20)
            for result in results:
                print(result["filepath"])
        elif option == 7:
            # Get distinct dir_names from regex matches
            results = queries.getDistinctDirNameByRegex(collection_name, regex, case_sensitive)
            for result in results:
                print(result)
        elif option == 8:
            levels_num = input("Enter number of levels to search:\n")
            levels_num = int(levels_num)
            if case_sensitive:
                match_stage = {"filepath": {"$regex": regex}}
            else:
                match_stage = {"filepath": {"$regex": regex, "$options": "i"}}
            results = queries.getAnalysisByDistinctDirectories(collection_name, levels_num, match_stage)
            for result in results:
                for value in result["distinctValues"]:
                    print("/".join(value))
        elif option == 9:
            
            print("Searching all regex matches")
            results = queries.getRegexMatches(collection_name, regex, case_sensitive, limit = 9999999)
            output_file_path = os.path.join(output_dir_path, f"{collection_name}_{regex}.txt")
            print(f"Writing to {output_file_path}")
            with open(output_file_path, "w") as f:
                for result in results:
                    f.write(result["filepath"] + "\n")
        elif option == 10:
            
            print("Searching distinct parent directories of regex matches")
            # Get distinct dir_names from regex matches
            results = queries.getDistinctDirNameByRegex(collection_name, regex, case_sensitive)
            output_file_path = os.path.join(output_dir_path, f"parent_{collection_name}_{regex}.txt")
            print(f"Writing to {output_file_path}")
            with open(output_file_path, "w") as f:
                for result in results:
                    f.write(result + "\n")
        elif option == 11:
            prev_regex = regex
            input_file_path = input("Enter input file path:\n")
            
            print("Searching regex by list")
            with open(input_file_path, "r") as f:
                regex_list = f.readlines()
            regex_list = [regex.strip() for regex in regex_list]
            for i,regex in enumerate(regex_list):
                print(f"Searching for {regex}")
                results = queries.getRegexMatches(collection_name, regex, case_sensitive, limit = 9999999)
                output_file_path = os.path.join(output_dir_path, f"{str(i+1)}_{collection_name}_{regex}.txt")
                print(f"Writing to {output_file_path}")
                with open(output_file_path, "w") as f:
                    for result in results:
                        f.write(result["filepath"] + "\n")
            regex = prev_regex
        elif option == 12:
            prev_regex = regex
            input_file_path = input("Enter input file path:\n")
            
            print("Searching regex by list")
            with open(input_file_path, "r") as f:
                regex_list = f.readlines()
            regex_list = [regex.strip() for regex in regex_list]
            for i,regex in enumerate(regex_list):
                print(f"Searching distinct parent directories of {regex}")
                # Get distinct dir_names from regex matches
                results = queries.getDistinctDirNameByRegex(collection_name, regex, case_sensitive)
                output_file_path = os.path.join(output_dir_path, f"{i+1}_parent_{collection_name}_{regex}.txt")
                print(f"Writing to {output_file_path}")
                with open(output_file_path, "w") as f:
                    for result in results:
                        f.write(result + "\n")
            regex = prev_regex





def comparisonMenu():

    def findFileNamesNotInTACC():
        options = [
            "Exit",
            "Other",
            "/share/projdir",
            "/share/aserv01",
            "/share/aserv00",
            "/net/vstor/export/vstor1",
            "/share/pdata*",
            "/share/tstor0"
        ]

        values = [
            None,
            None,
            "/share/projdir",
            "/share/aserv01.*",
            "/share/aserv00.*",
            "/net/vstor/export/vstor1.*",
            "/share/pdata.*",
            "/share/tstor0.*"
            
        ]
        option = menus.get_option_main(options)
        if option == 0:
            return
        elif option == 1:
            # Get starting base path
            print("Enter regex")
            regex = input()
        else:
            regex = values[option]

        print("Running Aggregation")
        # Get all filepaths in src_listing
        documents_list = queries.getANotinBByFilenameFiltered(regex, table_names["src_listing"], table_names["dst_listing"], ["f", "l", "lf"])
        if documents_list == []:
            print(f"No results found for {regex}")
            return
        else:
            # Define collection to end up in
            db = general.connectToDB(database_name)
            o_collection = db[table_names["src_not_in_dst"]]
            # Reset collection
            o_collection.drop()
            print("Inserting results")
            o_collection.insert_many(documents_list)


    def compareTwoDirectories():
        option = input("Compare radar directories? (y/n)")
        if option == "y":
            src_path = "/proj/radar"
            dest_path = "/stornext/ranch_103/ranch/projects/Arecibo-Observatory/Legacy/Sciences/Planetary-Radar"

        elif option == "n":
            # Ask for src path
            print("Enter source path")
            src_path = input()
            # Ask for dest path
            print("Enter destination path")
            dest_path = input()

        print("Getting comparisons")
        
        arguments = [
            (src_path, dest_path, table_names["src_listing"], table_names["dst_listing"]),
            # (dest_path, src_path, table_names["dst_listing"], table_names["src_listing"]),
            # (src_path, dest_path, table_names["src_listing"], table_names["dst_listing"], 0),
            # (dest_path, src_path, table_names["dst_listing"], table_names["src_listing"], 0)
        ]
        tables = [
            table_names["src_in_dst"],
            # table_names["dst_in_src"],
            # table_names["src_not_in_dst"],
            # table_names["dst_not_in_src"]
        ]
        queries.getAInBByFilepath(*arguments[0])
        
        results = submitInParallel(queries.getAInBByFilepath, arguments)

        breakpoint()
        # Insert into tables
        print("Inserting into tables")
        run_list = []
        for i, result in enumerate(results):
            run_list.append((result, tables[i]))

        for run in run_list:
            print(f"Inserting into {run[1]}")
            
            db = general.connectToDB(database_name)
            collection = db[run[1]]
            collection.insert_many(run)



    while True:
        options = [
            "Return to main menu",
            "Find files in src whose filenames are found nowhere in dst",
            "Compare two directories directly"
        ]
        functions = [
            None,
            findFileNamesNotInTACC,
            compareTwoDirectories
        ]
        option = menus.get_option_main(options)
        if option == 0:
            return
        else:
            functions[option]()

def analysisMenu():
    
    def analyzeByDirName():
        # Get which collection to analyze
        print("Which collection do you want to analyze?")
        options = [
            "Return to main menu",
            table_names["src_not_in_dst"],
        ]
        option = menus.get_option_main(options)
        if option == 0:
            return
        else:
            collection_name = options[option]
        
        print("Do you want to output to file?")
        options = [
            "Yes",
            "No"
        ]
        option = menus.get_option_main(options)
        output_dir_path = ""
        output_file_path = ""
        if option == 0:
            output_dir_path = input("Please enter the output dir path:\n")
            filename = "parent_dirs_analysis.txt"
            output_file_path = os.path.join(output_dir_path, filename)


        # Create db connection
        
        results = queries.getDistinctValues(collection_name, "dir_name")
        for result in results:
            if output_file_path != "":
                with open(output_file_path, "a") as f:
                    f.write(result["dir_name"] + "\n")
            else:
                print(result["dir_name"])
        
    print("Select which analysis to perform")
    options = [
        "Return to main menu",
        "Analyze by Parent Directory"
    ]
    functions = [
        None,
        analyzeByDirName
    ]
    option = menus.get_option_main(options)
    if option == 0:
        return
    else:
        functions[option]()


def deleteListingEntries():
    # connect to DB
    db = general.connectToDB(database_name)

    # get all documents in listing_paths
    collection = db[table_names["listing_paths"]]
    listing_paths = collection.find({}, {"_id":1,"filepath": 1})

    print("Select which listing to delete data from")
    options = [
        "Return to main menu",
        "Source Listing",
        "Destination Listing"
    ]
    option = menus.get_option_main(options)
    if option == 0:
        return
    elif option == 1:
        table_name = table_names["src_listing"]
        listing_paths = [listing_path for listing_path in listing_paths if listing_path["filepath"].startswith(source_dir_path)]
    elif option == 2:
        table_name = table_names["dst_listing"]
        listing_paths = [listing_path for listing_path in listing_paths if listing_path["filepath"].startswith(destination_dir_path)]
    else:
        print("Invalid option")
        return

    # Print all options and let user choose which to delete
    options = []
    for listing_path in listing_paths:
        options.append(listing_path["filepath"])
    option = menus.get_option_main(options)
    delete_doc = listing_paths[option]

    # Delete all documents in table_name where listing_paths_ID = delete_doc["_id"]
    queries.deleteByListingPathsID(table_name, delete_doc["_id"])

    print("Deleting from listing_paths")
    # Delete the document from listing_paths
    collection.delete_one({"_id": delete_doc["_id"]})




def backupDB():
    global backup_dir_path 
    global max_backup_size_GB
    global database_name
    # Get total size
    # Get all files
    files = general.getRecursiveFiles(backup_dir_path)
    total_size_GB = 0
    for file in files:
        total_size_GB += os.path.getsize(file)/10**9
 
    if total_size_GB > max_backup_size_GB:
        print("MAX SIZE REACHED, PLEASE CODE SOMETHING TO DELETE FILES")
        return
    
    # Create backup file name with date and time
    now = datetime.now()
    timestamp = now.strftime("%Y_%m_%d_%H_%M_%S")
    output_dir_name = f"backup_{timestamp}"
    output_dir_path = os.path.join(backup_dir_path, output_dir_name)

    command = f"mkdir {output_dir_path}"
    subprocess.call(command, shell=True)

    # backup DB
    command = f"mongodump --host localhost --port 27017 --db {database_name} --out {output_dir_path}"
    print(command)
    # run command
    subprocess.call(command, shell=True)

def restoreDB():
    global backup_dir_path 
    global max_backup_size_GB
    global db_connection_info

    # Get files
    files = glob.glob(os.path.join(backup_dir_path, "*"))
    option = menus.get_option_main(files)
    file_path = files[option]



    # restore DB
    command = f"mysql -u {db_connection_info['user']} -p{db_connection_info['passwd']} {db_connection_info['database']} < {file_path}"

    # run command
    subprocess.call(command, shell=True)

def runTests():
    options = [
        "Return to main menu",
        "Test submitParallel"
    ]
    functions_list = [
        None,
        testParallel,
    ]
    option = menus.get_option_main(options)
    if option == 0:
        return
    else:
        functions_list[option]()

def runInsertFileDir():
    args = [
        (table_names["src_listing"], table_names["src_file_dir"]),
        (table_names["dst_listing"], table_names["dst_file_dir"])
    ]

    submitInParallel(insert_file_dir, args)





def delete_file_sql_contents():
    
    # Make user type the name oif the file to delete, if not correct, ask again
    options = [
        "Return to main menu",
        "Source_Listing",
        "Destination_Listing"
    ]
    print("-----------DELETE FILE FROM SQL-----------")
    option = menus.get_option_main(options)

    if option == 0:
        return
    
    file_name = input("Enter the name of the file to delete: ")

    if option == 1:
        check_dir = source_dir_path
        table_name = table_names["src_listing"]
    else:
        check_dir = destination_dir_path
        table_name = table_names["dst_listing"]
    
    # get finished files
    finished_dir = os.path.join(check_dir, "finished")
    files = glob.glob(os.path.join(finished_dir, '*.txt'))

    # check which file to delete
    try:
        index = files.index(os.path.join(finished_dir,file_name))
    except ValueError:
        print("File not found")
        return

    chosen_file = files[index]  

    file_mv_path = chosen_file.split("/")
    file_mv_path.pop(-2)
    file_mv_path = "/".join(file_mv_path)

    file_path_ID = queries.get_ID_by_filepath(mydb, file_mv_path, table_names["listing_paths"])
    # delete file from sql table
    queries.delete_by_ID(mydb, file_path_ID, table_name, "listing_paths_ID")
    # delete from listing paths
    queries.delete_by_ID(mydb, file_path_ID, table_names["listing_paths"], "ID")

    # move file back to root folder
    shutil.move(chosen_file, check_dir)

    print(f"File {chosen_file} deleted from sql table")

def create_mapping():
    print("Creating mapping")

    # Get all files in the source listing not yet in mapping
    query = queries.select_file_names_no_relations.format(table_name=table_names["src_listing"], mapping_table_name=table_names["mapping"])
    mycursor = submitQuery(query)
    myresult = mycursor.fetchall()
    mycursor.close()
    for row in myresult:
        # Get all files in destination listing that match the filename  and insert the results into the mapping table
        query = queries.insert_mapping_filename.format(mapping_table_name=table_names["mapping"],src_table_name=table_names["src_listing"], dst_table_name=table_names["dst_listing"], src_ID=row[0], filename=row[1])        
        mycursor = submitQuery(query)











    


    

    






 





def run_imports():
# Move all destination listing_dirs in the finished folder to the root folder
    finished_listing_dirs = get_listing_dirs(os.path.join(source_dir_path, "finished"))
    
    for directory in finished_listing_dirs:
        command = f"mv {directory} {destination_dir_path}"
        os.system(command)
        
    # Move all destination listing_dirs in the finished folder to the root folder
    finished_listing_dirs = get_listing_dirs(os.path.join(destination_dir_path, "finished"))
    
    for directory in finished_listing_dirs:
        command = f"mv {directory} {destination_dir_path}"
        os.system(command)
   
    args = [
        (source_dir_path, table_names["src_listing"], 0),
        (destination_dir_path, table_names["dst_listing"], 1)
    ]
    submitInParallel(import_data, args)
    





def run_insert_file_dir():

    # submit in parallel
    args = [
        (table_names["src_listing"], table_names["src_file_dir"]),
        (table_names["dst_listing"], table_names["dst_file_dir"])
    ]
    submitInParallel(insert_file_dir, args)
        

def insert_file_dir(table_name, file_dir_table_name):
    print(f"Inserting file dir relations for {table_name}")

    query = queries.insert_file_dir.format(table_name=table_name, file_dir_relations_table_name=file_dir_table_name)
    
    executeQuery(query)
    print(f"Finished inserting file dir relations for {table_name}")





def executeQuery(query):
    global db_connection_info
    new_mydb = mysql.connector.connect(
    host=db_connection_info["host"],
    user=db_connection_info["user"],
    passwd=db_connection_info["passwd"],
    database=db_connection_info["database"],
    allow_local_infile=db_connection_info["allow_local_infile"])

    mycursor = new_mydb.cursor()
    mycursor.execute(query)
    mycursor.close()
    new_mydb.commit()
    
    return mycursor




# random functions
def get_listing_dirs(dir_path):
    # Get all directories in the dir_path starting with _
    listing_dirs = glob.glob(os.path.join(dir_path, '_*'))

    # filter out any directories that are not directories
    listing_dirs = [dir for dir in listing_dirs if os.path.isdir(dir)]

    return listing_dirs

def randomReturn(i):
    # get random number between 0 and 10
    rand_num = random.randint(1,10)
    time.sleep(rand_num)
    return i

def testParallel():
    arguments = []
    for i in range(100):
        arguments.append((i,))
    results = submitInParallel(randomReturn, arguments)
    print(results)



def submitInParallel(function,args_list, get_results = True, check_progress = True):
    check_time = 2
    # check_time = 1
    p_list = []
    pool = mp.Pool(processes=mp.cpu_count()*10)

    # Monitor the memory usage of a single process, use the average to estimate the amount of processes you can run
    
    for arg in args_list:
        p_list.append(pool.apply_async(function, arg))
        
    print("\n\n\nAll processes started: " + str(len(p_list)) + "\n\n")
    start_time = time.time()
    results = []
    # fill results with None values
    for i in range(len(p_list)):
        results.append(None)
    
    finished_plist = []
    last_finished_plist_len = 0
    while len(p_list) != len(finished_plist):
        finished_plist_len = len(finished_plist)

        
        if finished_plist_len != last_finished_plist_len:
            finished_num = finished_plist_len - last_finished_plist_len
            finished_plist_len = len(finished_plist)
            left_num = len(p_list) - finished_plist_len
            print(f"\nFinished {finished_num} processes")
            print(f"Processes left: {len(p_list) - finished_plist_len}")
            print(f"Time elapsed: {time.time() - start_time} seconds")
            print("Average time per process: " + str((time.time() - start_time)/left_num) + " seconds")
            print("Estimated time left: " + str((time.time() - start_time)/finished_num * left_num) + " seconds\n")
            start_time = time.time()
            
        print("Checking processes")
        for i, p in enumerate(p_list):
            
            if p in finished_plist:
                continue
            if check_progress:
                if p.ready():
                    if get_results:
                        results[i] = p.get()
                    finished_plist.append(p)
            else:
                if get_results:
                    results[i] = p.get()
                    finished_plist.append(p)
                else:
                    p.get()
                    finished_plist.append(p)
            if len(finished_plist) % 10000 == 0:
                print("Still checking, but finished " + str(len(finished_plist)) + " processes")
        last_finished_plist_len = finished_plist_len
        print("Sleeping for " + str(check_time) + " seconds")
        time.sleep(check_time)

    pool.close()
    pool.join()


    if get_results:
        return results
    else:
        return None



main()