
# Software to import data from a csv or txt file into a mySQL table and compare the tables to check for missing files.
# Emanuel Rodriguez
# 2023-03-23
# Arecibo Observatory Big Data  

# TODO:
# - Add src_listing option
# - Add, get broken_links
# - Add missing_listing_dirs option
# All of above in listing options
# - Add option to resolve links to ID (Added but needs better testing before release)
# - Add option to move folders in sql DB using like
# - Add option to move files in sql DB using like
# - Add option to generate reports

import pymongo

import mysql.connector
import os
import glob
from Modules import queries
from Modules import menus
from Modules import make_blacklist
from Modules import global_vars
from Modules import general
from Modules import import_data
import shutil
import subprocess
import time
import pandas as pd
import multiprocessing as mp
from datetime import datetime

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




#Setup vaiables
################
table_names = {
    "src_listing": "src_listing",
    "src_file_dir": "src_file_dir_relations",

    "dst_listing": "dst_listing",
    "dst_file_dir": "dst_file_dir_relations",
    "listing_paths": "listing_paths",
    "blacklist": "blacklist_pattern",
    "blacklist_relations": "src_listing_has_blacklist_pattern"

}
global_vars.table_names = table_names
################

def main():

    run_dict = {
        "options": [
            "Quit",
            "Reset DB",
            "Import New Data",

            
        ],
        "functions": [
            quit,
            run_resets,
            importNewData,


            
        ]
            
    }

    while True:
        print("-----------Main Menu-----------")
        option = menus.get_option_main(run_dict["options"])
        run_dict["functions"][option]()


def backupDB():
    global backup_dir_path 
    global max_backup_size_GB
    global db_connection_info
    # Get total size
    # Get all files
    files = glob.glob(os.path.join(backup_dir_path, "*"))
    total_size_GB = 0
    for file in files:
        total_size_GB += os.path.getsize(file)/10**9
 
    if total_size_GB > max_backup_size_GB:
        print("MAX SIZE REACHED, PLEASE CODE SOMETHING TO DELETE FILES")
        return
    
    # Create backup file name with date and time
    now = datetime.now()
    timestamp = now.strftime("%Y_%m_%d_%H_%M_%S")
    output_filename = f"backup_{timestamp}.sql"
    output_file_path = os.path.join(backup_dir_path, output_filename)

    # backup DB
    command = f"mysqldump -u {db_connection_info['user']} -p{db_connection_info['passwd']} {db_connection_info['database']} > {output_file_path}"
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
    

def runInsertFileDir():
    args = [
        (table_names["src_listing"], table_names["src_file_dir"]),
        (table_names["dst_listing"], table_names["dst_file_dir"])
    ]

    submitInParallel(insert_file_dir, args)

def generate_report():
    print("--------Generate Report--------")
    def end_loop():
        
        return True

    def dirs_missing_report():
        # get 10 rows of missing_dirs
        query = queries.get_missing_included_files.format(table_name = table_names["src_listing"])
        myresult = submitQuery(query, False, True)

        # turn dictionary to dataframe
        df = pd.DataFrame(myresult)
        print(df)

        return False



    options = [[
        "Back to Main Menu",
        "Get src dirs missing, using points_to"
    ],
    [
        end_loop,
        dirs_missing_report
    ]
    ]   
    finished = False
    while not finished: 
        option = menus.get_option_main(options[0])
        finished = options[1][option]()




def insert_new_files():
    print("Inserting new source files")
    # get only files not in finished folder
    src_dirs = get_listing_dirs(source_dir_path)
    dst_dirs = get_listing_dirs(destination_dir_path)

    args = [
        (source_dir_path, table_names["src_listing"],0),
        (destination_dir_path, table_names["dst_listing"],1)
    ]

    submitInParallel(import_data, args)

     # convert points_to to absolute_paths
    convert_relative_to_absolute()

    # Identify actual broken links
    add_broken_links()

    # Resolve points_to to ID
    resolve_links_to_ID()

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



def move_folder():
    # Ask src or dst
    options = [
        "Return to main menu",
        "Source",
        "Destination"
    ]
    print("-----------MOVE FOLDER-----------")
    option = menus.get_option_main(options)
    table_name = ""
    if option == 0:
        return
    elif option == 1:
        table_name = table_names["src_listing"]
    elif option == 2:
        table_name = table_names["dst_listing"]

    # Get the filepath to move
    dir_path = input("Enter the dirpath to move: ")

    # Get where to move it to
    new_dir_path = input("Enter the new path: ")

    # Check if the old path exists in DB
    query = queries.get_dir_by_filepath.format(table_name=table_name, filepath=dir_path)
    
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    mycursor.close()
    if len(myresult) == 0:
        print("Directory not found")
        return
    elif len(myresult) > 1:
        print("Multiple directories found")
        return
    breakpoint()

def convert_relative_to_absolute():
    print("Converting points_to to absolute paths")
    # Get all lines where points_to does not start with /
    query = queries.get_links_points_to_not_absolute.format(table_name=table_names["src_listing"])
    results = submitQuery(query, False, True)
    if len(results) == 0:
        return

    for row in results:
        # So that pylance doesn't complain
        row = dict(row)
        # change points_to to absolute
        absolute_path = os.path.abspath(os.path.join(row['filepath'], row['points_to']))
        row['points_to'] = absolute_path
        # update row
        update_query = queries.update_link_points_to.format(table_name=table_names["src_listing"], points_to=row['points_to'], ID=row['ID'])
        results = submitQuery(update_query, True, True)

def add_broken_links():
    print("Adding broken links")
    # Create broken links report
    # Get all links in src_listing
    query = queries.get_null_broken_links.format(table_name=table_names["src_listing"])

    src_links = submitQuery(query, False, False)
    print(f"Fetched {len(src_links)} links to check")
    # open link_info file as write
    f = open(link_info_path, "w")
    # write header
    line = "ID\tpoints_to\tbroken_link\n"
    f.write(line)
    # iterate through links and check if they are broken
    for i, link_info in enumerate(src_links):
        line = ""
        for element in link_info:
            line += str(element) + "\t"
        # Take away trailing tab
        line = line[:-1]
        # Add endline
        line += "\n"
        f.write(line)
    f.close()
        

    # Check if the link is broken
    command = f"ssh -J remote.naic.edu -t transport 'cd {os.path.abspath(modules_path)};python3.7 check_links.py;'"
    print("Checking links on transport")
    
    subprocess.run(command, shell=True)

    # subprocess.call(command, shell=True)
    # Read new link_info file
    new_link_info_path = os.path.join(os.path.dirname(link_info_path), f"new_{os.path.basename(link_info_path)}")
    f = open(new_link_info_path, "r")  
    print("Updating broken links in DB")
    batch_size = 10 ** 4
    
    # process the file in batches using pandas
    for df in pd.read_csv(f, sep="\t", chunksize=batch_size):
        tasks = []
        # group by 0 and 1, add to tasks
        tasks.append(tuple(df[df["broken_link"] == 0]["ID"].to_list()))
        tasks.append(tuple(df[df["broken_link"] == 1]["ID"].to_list()))

        # update src_listing table
        for i, IDs in enumerate(tasks):
            if len(IDs) > 0:
                IDs_str = str(IDs)
                query = queries.update_broken_by_ID_list.format(table_name=table_names["src_listing"], value=i, ID_list=IDs_str)
                results = submitQuery(query, True, True)

                

    # delete both link_info files
    os.remove(link_info_path)
    os.remove(new_link_info_path)

def resolve_links_to_ID():
    print("Resolving links to ID")
    query = queries.update_fk_table_ID.format(table_name=table_names["src_listing"])
    args = [(query, True, True)]
    # Do the same for dst_listing
    query = queries.update_fk_table_ID.format(table_name=table_names["dst_listing"])
    args.append((query, True, True))
    submitInParallel(submitQuery, args)

    


    

    

def run_resets():
    # connect to mongoDB, drop all collections
    # connect to DB
    db = general.connectToDB(database_name)
    # drop all collections
    for collection in db.list_collection_names():
        db[collection].drop()


def importNewData():



    # run for dst for now

    # Get all files in destination listing
    files = glob.glob(os.path.join(destination_dir_path, '*.txt'))
    # Assign all files to dst_listing
    file_db_info = [[files, table_names["dst_listing"]]]
    import_data.run(file_db_info, database_name)





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

def submitInParallel(function,args_list):
    p_list = []
    for arg in args_list:
        p = mp.Process(target=function, args=arg)
        p.daemon = False
        p_list.append(p)
        p.start()

    for p in p_list:
        p.join()

def submitQuery(query, commit_bool, dictionary = False):
    global db_connection_info
    mydb = mysql.connector.connect(
    host=db_connection_info["host"],
    user=db_connection_info["user"],
    passwd=db_connection_info["passwd"],
    database=db_connection_info["database"],
    allow_local_infile=db_connection_info["allow_local_infile"])

    cursor = mydb.cursor(dictionary=dictionary)

    cursor.execute(query)
    results = cursor.fetchall()

    if commit_bool:
        mydb.commit() 

    return results



main()