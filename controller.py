#!/usr/bin/python3.7
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

import mysql.connector
import os
import glob
from Modules import queries
from Modules import menus
import shutil
import subprocess
import time
import pandas as pd
import multiprocessing as mp

dir_listing_path = 'dir_listing/'

listings_path = 'file_listings/'
general_files_path = 'general_files/'
modules_path = 'Modules/'

link_info_path = os.path.join(general_files_path, 'link_info.tsv')

destination_dir_path = os.path.join(listings_path, 'Destination_Listing/')
source_dir_path = os.path.join(listings_path, 'Source_Listing/')

database = "Skittles_DB"

# connect to existing mySQL database
db_connection_info = {
    "host": "127.0.0.1",
    "user": "erodrigu",
    "passwd": "password",
    "database": database,
    "allow_local_infile": True
}
mydb = mysql.connector.connect(
host=db_connection_info["host"],
user=db_connection_info["user"],
passwd=db_connection_info["passwd"],
database=db_connection_info["database"],
allow_local_infile=db_connection_info["allow_local_infile"])

print("Connected to database")


#Setup vaiables
################
table_names = {
    "src_listing": "src_listing",
    "src_file_dir": "src_file_dir_relations",

    "dst_listing": "dst_listing",
    "dst_file_dir": "dst_file_dir_relations",

    "listing_paths": "listing_paths",
    "mapping": "src_dst_mapping"
}
################

def main():

    run_dict = {
        "options": [
            "Quit",
            "Run setup",
            "Gererate Report",
            "Import new files",
            "Delete file contents from sql table",
            "Create Mapping",
            "Move Folder",
            
        ],
        "functions": [
            quit,
            setup,
            generate_report,
            insert_new_files,
            delete_file_sql_contents,
            create_mapping,
            move_folder,

            
        ]
            
    }
    
    finished = False
    while not finished:
        print("-----------Main Menu-----------")
        option = menus.get_option_main(run_dict["options"])
        run_dict["functions"][option]()


def setup():

    # Run listing scripts in transport
    command = f"ssh -J remote.naic.edu -t transport 'cd {os.path.abspath(dir_listing_path)}; python3.7 listing.py'"
    os.system(command)
    
    run_resets()
    run_imports()
    


    insert_file_dir()

    # convert points_to to absolute_paths
    convert_relative_to_absolute()

    # Identify actual broken links
    add_broken_links()

    # Resolve points_to to ID
    resolve_links_to_ID()

    # move files to finished folder
    files = glob.glob(os.path.join(source_dir_path, '*.txt'))
    for file in files:
        shutil.move(file, os.path.join(source_dir_path, "finished"))

    files = glob.glob(os.path.join(destination_dir_path, '*.txt'))
    for file in files:
        shutil.move(file, os.path.join(destination_dir_path, "finished"))
    
def generate_report():
    print("--------Generate Report--------")
    def end_loop():
        
        return True

    def dirs_missing_report():
        # get 10 rows of missing_dirs
        query = queries.get_missing_included_files.format(table_name = table_names["src_listing"])
        mycursor = mydb.cursor(dictionary=True)
        mycursor.execute(query)
        myresult = mycursor.fetchall()
        mycursor.close()

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
    pattern = '.txt'
    files = [os.path.join(source_dir_path,filename) for filename in os.listdir(source_dir_path) if filename.endswith(pattern)]

    import_data(source_dir_path, table_names["src_listing"])

    import_data(destination_dir_path, table_names["dst_listing"])

     # convert points_to to absolute_paths
    convert_relative_to_absolute()

    # Identify actual broken links
    add_broken_links()

    # Resolve points_to to ID
    resolve_links_to_ID()

    # move files to finished folder
    for file in files:
        # move file to finished folder
        shutil.move(file, os.path.join(source_dir_path, "finished"))

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
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    mycursor.close()
    for row in myresult:
        # Get all files in destination listing that match the filename  and insert the results into the mapping table
        query = queries.insert_mapping_filename.format(mapping_table_name=table_names["mapping"],src_table_name=table_names["src_listing"], dst_table_name=table_names["dst_listing"], src_ID=row[0], filename=row[1])        
        mycursor = mydb.cursor()
        mycursor.execute(query)
        mydb.commit()



def move_folder():
    # Ask src or dst
    options = [
        "Return to main menu",
        "Source",
        "Destination"
    ]
    print("-----------MOVE FOLDER-----------")
    option = menus.get_option_main(options)

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
    
    # Get all lines where points_to does not start with /
    query = queries.get_links_points_to_not_absolute.format(table_name=table_names["src_listing"])
    mycursor = mydb.cursor(dictionary=True)
    mycursor.execute(query)
    results = mycursor.fetchall()
    mycursor.close()
    for row in results:
        # change points_to to absolute
        absolute_path = os.path.abspath(os.path.join(row['filepath'], row['points_to']))
        row['points_to'] = absolute_path
        # update row
        update_query = queries.update_link_points_to.format(table_name=table_names["src_listing"], points_to=row['points_to'], ID=row['ID'])
        mycursor = mydb.cursor(dictionary=True)
        mycursor.execute(update_query)
        mydb.commit()
        mycursor.close()

def add_broken_links():
    print("Adding broken links")
    # Create broken links report
    # Get all links in src_listing
    query = queries.get_null_broken_links.format(table_name=table_names["src_listing"])
    mycursor = mydb.cursor()
    mycursor.execute(query)
    src_links = mycursor.fetchall()
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
    
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
                mycursor = mydb.cursor()
                mycursor.execute(query)
                mydb.commit()
                mycursor.close()
                

    # delete both link_info files
    os.remove(link_info_path)
    os.remove(new_link_info_path)

def resolve_links_to_ID():
    print("Resolving links to ID")
    query = queries.update_fk_table_ID.format(table_name=table_names["src_listing"])
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    mycursor.close()

    # Do the same for dst_listing
    query = queries.update_fk_table_ID.format(table_name=table_names["dst_listing"])
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    mycursor.close()


    

    

def run_resets():
    table_list = [table_names[key] for key in table_names]
    queries.delete_tables_data(mydb, table_list, database)

def run_imports():
    # # Move all files in the finished folder to the root folder
    # files = glob.glob(os.path.join(source_dir_path, "finished", '*.txt'))
    # for file in files:
    #     shutil.move(file, source_dir_path)
    # start_time = time.time()
    # import_data(source_dir_path, table_names["src_listing"])
    # print("Imported source files in {} seconds".format(time.time() - start_time))


    # Move all destination files in the finished folder to the root folder
    files = glob.glob(os.path.join(destination_dir_path, "finished", '*.txt'))
    for file in files:
        shutil.move(file, destination_dir_path)

    start_time = time.time()    
    import_data(destination_dir_path, table_names["dst_listing"])
    print("Imported destination files in {} seconds".format(time.time() - start_time))
    quit()



def insert_file_dir():
        
    print("Inserting file dir relations for src_files")
    # loop through results of query
    mycursor = mydb.cursor()
    query = queries.select_dir_names_no_relations.format(table_name=table_names["src_listing"], file_dir_table_name=table_names["src_file_dir"])
    
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    for row in myresult:
        # insert filepath in table
        dir_ID = row[0]
        filepath = row[1]
        print(filepath)

        mycursor = mydb.cursor()
        if "'" in filepath:
            query = queries.insert_file_dir_d_q.format(src_table_name= table_names['src_listing'],dst_table_name=table_names["src_file_dir"], filepath=filepath, dir_ID=dir_ID)
        else:
            query = queries.insert_file_dir_q.format(src_table_name= table_names['src_listing'],dst_table_name=table_names["src_file_dir"], filepath=filepath, dir_ID=dir_ID)
        mycursor.execute(query)
        mydb.commit()
    print("Inserting file dir relations for dst_files")
    mycursor = mydb.cursor()
    query = queries.select_dir_names_no_relations.format(table_name=table_names["dst_listing"], file_dir_table_name=table_names["dst_file_dir"])
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    for row in myresult:
        # insert filepath in table
        filepath = row[1]
        dir_ID = row[0]
        print(filepath)
        mycursor = mydb.cursor()

        if "'" in filepath:
            query = queries.insert_file_dir_d_q.format(src_table_name= table_names['dst_listing'],dst_table_name=table_names["dst_file_dir"], filepath=filepath, dir_ID=dir_ID)
        else:
            query = queries.insert_file_dir_q.format(src_table_name= table_names['dst_listing'],dst_table_name=table_names["dst_file_dir"], filepath=filepath, dir_ID=dir_ID)



        mycursor.execute(query)
        mydb.commit()
    print("Finished inserting file dir relations")





def import_data(dir_path, table_name):

    pattern = '.txt'
    files = [os.path.join(dir_path, filename) for filename in os.listdir(dir_path) if filename.endswith(pattern)]

    if len(files) == 0:
        print(f"No files found in {dir_path}")
        return
    
    print(f"Importing {dir_path} files")
    # get only files not in finished folder
    # ...
    results = queries.prepare_table_import(mydb, table_name, db_connection_info)
    fk_results = results[0]
    fk_info = results[1]
    
    # print files
    [print(file) for file in files]
    # run in parallel
    arguments = [(db_connection_info, file, table_name, fk_results) for file in files]
    with mp.Pool() as pool:
        pool.starmap(queries.import_data, arguments)
    
    queries.finalize_table_import(mydb, fk_info, table_name)



    # pattern = '.txt'
    # files = [os.path.join(dir_path,filename) for filename in os.listdir(dir_path) if filename.endswith(pattern)]

    # if len(files) == 0:
    #     print(f"No files found in {dir_path}")
    #     return
    # print(f"Importing {dir_path} files")
    #  # get only files not in finished folder
    
    # # run in parallel
    # arguments = []
    # for file in files:
    #     print(f"Importing file: {file}")
    #     queries.import_data(mydb, file, table_name, database) 
    # #     arguments.append((mydb, file, table_name, database))
         
    # # with mp.Pool() as pool:
    # #     pool.starmap(queries.import_data, arguments)

main()