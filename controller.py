#!/usr/bin/python3
# Software to import data from a csv or txt file into a mySQL table and compare the tables to check for missing files.
# Emanuel Rodriguez
# 2023-03-23
# Arecibo Observatory Big Data  

import mysql.connector
import os
import glob
import sys
from Modules import queries
from Modules import menus
import shutil
import time

listings_path = 'file_listings/'

destination_dir_path = os.path.join(listings_path, 'Destination_Listing/')
source_dir_path = os.path.join(listings_path, 'Source_Listing/')


# connect to existing mySQL database
mydb = mysql.connector.connect(
host="127.0.0.1",
user="erodrigu",
passwd="password",
database="Skittles_DB",
allow_local_infile=True)

print("Connected to database")


#Setup vaiables
################

################



table_names = {
    "src_files": "src_file_listing",
    "src_dirs": "src_dirs",
    "src_links": "src_broken_links",
    "src_file_dir": "src_file_dir_relations",

    "dst_files": "dst_file_listing",
    "dst_file_dir": "dst_file_dir_relations",
    "dst_dirs": "dst_dirs"
}


def main():

    run_dict = {
        "options": [
            "Quit",
            "Run setup",
            "Import new files",
            "Delete file contents from sql table",
            
        ],
        "functions": [
            quit,
            setup,
            insert_new_files,
            delete_file_sql_contents

            
        ]
            
    }
    
    finished = False
    while not finished:
        print("-----------Main Menu-----------")
        option = menus.get_option_main(run_dict["options"])
        run_dict["functions"][option]()


    



# # Insert data into dst_dirs table
# mycursor3 = mydb.cursor()
# mycursor3.execute("INSERT INTO dst_dirs SELECT dst_listing.FilePath, dst_listing.FileName FROM dst_listing where filetype <> 'f';")
# mydb.commit()
# print("Data inserted into dst_dirs")

# mycursor4 = mydb.cursor()
# mycursor4.execute("INSERT INTO src_dirs SELECT src_listing.filepath, src_listing.filename FROM src_listing where filetype <> 'f';")
# mydb.commit()
# print("Data inserted into src_dirs")

# # delete rows from destination and source that are not files
# mycursor5 = mydb.cursor()
# mycursor5.execute("delete from Skittles_DB.dst_listing where filetype <> 'f'")
# mydb.commit()
# print("Deleted rows from dst_listing")

# mycursor6 = mydb.cursor()
# mycursor6.execute("delete from Skittles_DB.src_listing where filetype <> 'f'")
# mydb.commit()
# print("Deleted rows from src_listing")

# # Compare tables
# mycursor7 = mydb.cursor(buffered=True)
# mycursor7.execute("INSERT INTO src_missing select src_listing.filename, src_listing.filepath, src_listing.FileType, src_listing.Filesize, src_listing.FileAtime, src_listing.FileMtime, src_listing.FileCtime from src_listing left join dst_listing on src_listing.filename = dst_listing.Filename where dst_listing.Filename is null;")
# mydb.commit()
# print("Query for comparison executed")

# # close connection
# mydb.close()
# print("Finished")

def setup():
    
    run_resets()

    run_imports()
    

    insert_dirs()

    insert_src_links()

    insert_file_dir()

    # move files to finished folder
    files = glob.glob(os.path.join(source_dir_path, '*.tsv'))
    for file in files:
        shutil.move(file, os.path.join(source_dir_path, "finished"))

    files = glob.glob(os.path.join(destination_dir_path, '*.tsv'))
    for file in files:
        shutil.move(file, os.path.join(destination_dir_path, "finished"))


def insert_new_files():
    print("Inserting new source files")
    # get only files not in finished folder
    pattern = '.tsv'
    files = [os.path.join(source_dir_path,filename) for filename in os.listdir(source_dir_path) if filename.endswith(pattern)]

    import_data(source_dir_path, table_names["src_files"])

    import_data(destination_dir_path, table_names["dst_files"])
    
    insert_dirs()
    insert_src_links()
    insert_file_dir()

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
    else:
        check_dir = destination_dir_path
    
    # get finished files
    finished_dir = os.path.join(check_dir, "finished")
    files = glob.glob(os.path.join(finished_dir, '*.tsv'))

    # check which file to delete
    try:
        index = files.index(os.path.join(finished_dir,file_name))
    except ValueError:
        print("File not found")
        return

    chosen_file = files[index]
    # delete file from sql table



    # Delete selected file from sql table
    # Re-print until user selects return to main menu
    # Get finished files from finished folder
    

def run_resets():
    table_list = [table_names[key] for key in table_names]
    queries.delete_tables_data(mydb, table_list)

def run_imports():
    finished_files = []
    # Move all files in the finished folder to the root folder
    files = glob.glob(os.path.join(source_dir_path, "finished", '*.tsv'))
    for file in files:
        shutil.move(file, source_dir_path)
    start_time = time.time()
    import_data(source_dir_path, table_names["src_files"])
    print("Imported source files in {} seconds".format(time.time() - start_time))
    # Move all files in the finished folder to the root folder
    files = glob.glob(os.path.join(destination_dir_path, "finished", '*.tsv'))
    for file in files:
        shutil.move(file, destination_dir_path)

    start_time = time.time()    
    import_data(destination_dir_path, table_names["dst_files"])
    print("Imported destination files in {} seconds".format(time.time() - start_time))

def insert_dirs():
    print("Inserting dirs into tables")

    # insert dirs into src_dirs table
    mycursor = mydb.cursor()
    query = queries.insert_type.format(src_table_name=table_names["src_files"], dst_table_name=table_names["src_dirs"], type="d")
    mycursor.execute(query)
    mydb.commit()

    # delete dirs from src_files table
    mycursor = mydb.cursor()
    query = queries.delete_type.format(table_name=table_names["src_files"], type="d")
    mycursor.execute(query)
    mydb.commit()


        
    # insert dirs into dst_dirs table
    mycursor = mydb.cursor()
    query = queries.insert_type.format(src_table_name=table_names["dst_files"], dst_table_name=table_names["dst_dirs"], type="d")
    mycursor.execute(query)
    mydb.commit()

    # delete dirs from dst_files table
    mycursor = mydb.cursor()
    query = queries.delete_type.format(table_name=table_names["dst_files"], type="d")
    mycursor.execute(query)
    mydb.commit()

def insert_src_links():
    print("Inserting links into tables")
    # insert links into src_links table
    mycursor = mydb.cursor()
    query = queries.insert_type.format(src_table_name=table_names["src_files"], dst_table_name=table_names["src_links"], type="l")
    mycursor.execute(query)
    mydb.commit()
    
    # delete links from src_files table
    mycursor = mydb.cursor()
    query = queries.delete_type.format(table_name=table_names["src_files"], type="l")
    mycursor.execute(query)
    mydb.commit()

def insert_file_dir():
        
    print("Inserting file dir relations for src_files")
    # loop through results of query
    mycursor = mydb.cursor()
    query = queries.select_dir_names_no_relations.format(dir_table_name=table_names["src_dirs"], file_dir_table_name=table_names["src_file_dir"])
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    for row in myresult:
        # insert filepath in table
        dir_ID = row[0]
        filepath = row[1]
        print(filepath)
        mycursor = mydb.cursor()
        query = queries.insert_file_dir.format(src_table_name= table_names['src_files'],dst_table_name=table_names["src_file_dir"], filepath=filepath, dir_ID=dir_ID)
        mycursor.execute(query)
        mydb.commit()

    print("Inserting file dir relations for dst_files")
    mycursor = mydb.cursor()
    query = queries.select_dir_names_no_relations.format(dir_table_name=table_names["dst_dirs"], file_dir_table_name=table_names["dst_file_dir"])
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    for row in myresult:
        # insert filepath in table
        filepath = row[1]
        dir_ID = row[0]
        print(filepath)
        mycursor = mydb.cursor()
        query = queries.insert_file_dir.format(src_table_name= table_names['dst_files'],dst_table_name=table_names["dst_file_dir"], filepath=filepath, dir_ID=dir_ID)
        mycursor.execute(query)
        mydb.commit()
    print("Finished inserting file dir relations")





def import_data(dir_path, table_name):
    print(f"Importing {dir_path} files")
     # get only files not in finished folder
    pattern = '.tsv'
    files = [os.path.join(dir_path,filename) for filename in os.listdir(dir_path) if filename.endswith(pattern)]
    for file in files:
        print(f"Importing file: {file}")
        queries.import_data(mydb, file, table_name)
        


        
        

        


main()