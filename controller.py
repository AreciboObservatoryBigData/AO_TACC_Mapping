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
import shutil

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
            "Run setup",
            "Import new src files",
            "Import new dst files",
            "Delete src files from sql table",
            "Delete dst files from sql table"
        ],
        "functions": [
            setup,
        ]
            
    }
    
    finished = False
    while not finished:
        option = get_option(run_dict["options"])
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
    breakpoint()

    run_imports()

    insert_dirs()

    insert_src_links()

    insert_file_dir()

def run_resets():
    table_list = [
        table_names["src_files"],
        table_names["src_dirs"],
        table_names["src_links"]

    ]
    reset_table(table_list)

    table_list = [
        table_names["dst_files"],
        table_names["dst_dirs"]
    ]
    reset_table(table_list)


def run_imports():
    # Move all files in the finished folder to the root folder
    files = glob.glob(os.path.join(source_dir_path, "finished", '*.txt'))
    for file in files:
        shutil.move(file, source_dir_path)
    import_data(source_dir_path, table_names["src_files"])

    # Move all files in the finished folder to the root folder
    files = glob.glob(os.path.join(destination_dir_path, "finished", '*.txt'))
    for file in files:
        shutil.move(file, destination_dir_path)
    import_data(destination_dir_path, table_names["dst_files"])

def get_option(options):
    print("Please select an option:")
    for i in range(len(options)):
        print(str(i) + " - " + options[i])
    option = input("Option: ")
    option = int(option)
    return option
        

def insert_src_links():
    if import_src:
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
        
        

def insert_dirs():
    if import_src:

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

    if import_dst:
            
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

def insert_file_dir():
    if import_src:
        print("Inserting file dir relations for src_files")
        # loop through results of query
        mycursor = mydb.cursor()
        query = queries.select_path_lists.format(table_name=table_names["src_dirs"])
        mycursor.execute(query)
        myresult = mycursor.fetchall()
        for row in myresult:
            # insert filepath in table
            filepath = row[0]
            mycursor = mydb.cursor()
            query = queries.insert_file_dir.format(src_table_name= table_names['src_files'],dst_table_name=table_names["src_file_dir"], filepath=filepath)
            mycursor.execute(query)
            mydb.commit()
    if import_dst:
        print("Inserting file dir relations for dst_files")
        mycursor = mydb.cursor()
        query = queries.select_path_lists.format(table_name=table_names["dst_dirs"])
        mycursor.execute(query)
        myresult = mycursor.fetchall()
        for row in myresult:
            # insert filepath in table
            filepath = row[0]
            mycursor = mydb.cursor()
            query = queries.insert_file_dir.format(src_table_name= table_names['dst_files'],dst_table_name=table_names["dst_file_dir"], filepath=filepath)
            mycursor.execute(query)
            mydb.commit()
    print("Finished inserting file dir relations")
            
        





def reset_table(table_names: list):
    for table_name in table_names:
        print(f"Deleting table {table_name}")
        # Truncate tables
        mycursor = mydb.cursor()
        query = queries.delete_table_data.format(table_name=table_name)
        mycursor.execute(query)
        mydb.commit()
        print(f"Deleted {table_name}")

def import_data(dir_path, table_name):
    print(f"Importing {dir_path} files")
    files = glob.glob(os.path.join(dir_path, '*.txt'))

    finished_dir_path = os.path.join(dir_path, "finished")
    for file in files:
        print(f"Importing file: {file}")
        # Execute SQL statement
        mycursor = mydb.cursor()
        query = queries.import_data.format(file=file, table_name=table_name)

        mycursor.execute(query)
        mydb.commit()

        
        # move to the finished directory
        shutil.move(file, os.path.join(finished_dir_path, os.path.basename(file)))

        


main()