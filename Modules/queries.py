import os
import mysql.connector

insert_type = "INSERT INTO {dst_table_name} SELECT * FROM {src_table_name} where filetype = '{type}';"

delete_type = "DELETE FROM {table_name} WHERE filetype = '{type}';"

select_dir_names_no_relations = "SELECT ID, filepath FROM {table_name} WHERE filetype = 'd' AND ID NOT IN(SELECT DISTINCT {table_name}_dir_ID FROM {file_dir_table_name});"

select_file_names_no_relations = "SELECT ID, filename FROM {table_name} WHERE filetype = 'f' AND ID NOT IN(SELECT DISTINCT {table_name}_ID FROM {mapping_table_name});"

insert_mapping_filename = "INSERT INTO {mapping_table_name} ({src_table_name}_ID, {dst_table_name}_ID) SELECT '{src_ID}' as '{src_table_name}_ID', ID  FROM {dst_table_name} WHERE filename = '{filename}' AND filetype = 'f';"

get_dir_by_filepath = "SELECT ID, filepath FROM {table_name} WHERE filepath = '{filepath}' AND filetype = 'd';"

insert_file_dir_q = "INSERT INTO {dst_table_name} ({src_table_name}_dir_ID, {src_table_name}_ID) SELECT '{dir_ID}' as 'dir_ID', ID  FROM {src_table_name} WHERE filepath LIKE '{filepath}%' AND filetype <> 'd';"

insert_file_dir_d_q = "INSERT INTO {dst_table_name} ({src_table_name}_dir_ID, {src_table_name}_ID) SELECT '{dir_ID}' as 'dir_ID', ID  FROM {src_table_name} WHERE filepath LIKE \"{filepath}%\" AND filetype <> 'd';"


get_link_null = "SELECT ID, filepath, points_to FROM {table_name} WHERE {table_name}_ID IS NULL AND filetype = 'l';"

get_file_by_points_to = "SELECT ID, filepath FROM {table_name} WHERE filepath = '{points_to}';"

update_link_ID = "UPDATE {table_name} SET {table_name}_ID = {ID} WHERE ID = {link_ID};"

get_null_broken_links = "SELECT ID, points_to, broken_link FROM {table_name} WHERE filetype = 'l' AND broken_link IS NULL;"

update_broken_by_ID_list = "UPDATE {table_name} SET broken_link = {value} WHERE ID IN {ID_list};"

get_links_points_to_not_absolute = "SELECT ID,filepath, points_to FROM {table_name} WHERE filetype = 'l' AND points_to NOT LIKE '/%';"

update_link_points_to = "UPDATE {table_name} SET points_to = '{points_to}' WHERE ID = {ID};"

get_missing_included_files = "SELECT * FROM {table_name} where filetype = 'l' and {table_name}_ID is null and broken_link = 0 LIMIT 10;"

update_fk_table_ID = '''
Update {table_name} L1 
JOIN {table_name} L2
ON L1.filepath = L2.points_to
SET L2.{table_name}_ID = L1.ID
WHERE L2.{table_name}_ID IS NULL;'''

def get_ID_by_filepath(mydb, filepath, table_name):
    # Get the ID of the file just inserted
    query = f"SELECT ID FROM {table_name} WHERE filepath = '{filepath}';"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    file_ID = myresult[0][0]
    return file_ID
def delete_by_ID(mydb, ID, table_name, column_name):
    # Delete all rows in the table_name where the listing_paths_ID is the file_ID
    query = f"DELETE FROM {table_name} WHERE {column_name} = {ID};"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()

def get_all_data(mydb, table_name):
    query = f"SELECT * FROM {table_name};"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    return myresult

def get_data_by_column_value(mydb, name, table_name,  column_name):
    query = f"SELECT * FROM {table_name} WHERE {column_name} = '{name}';"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    return myresult

def delete_tables_data(mydb, tables_list, database):
    # get all foreign keys
    query = f'''SELECT
  constraint_name,
  table_name,
  column_name,
  referenced_table_name,
  referenced_column_name,
  constraint_schema
FROM
  information_schema.key_column_usage WHERE constraint_schema = "{database}";'''

    mycursor = mydb.cursor()

    mycursor.execute(query)
    myresult = mycursor.fetchall()
    foreign_keys= [row for row in myresult if "fk" in row[0]]
    
    
    print("Dropping all foreign keys")
    # drop all foreign keys
    for key_row in foreign_keys:
        key_name = key_row[0]
        key_table_name = key_row[1]
        query = f"ALTER TABLE {key_table_name} DROP FOREIGN KEY {key_name};"
        mycursor = mydb.cursor()
        mycursor.execute(query)

    print("Truncating Tables")
    # Truncate table
    for table_name in tables_list:
        print(f"Truncating {table_name}")
        query = f"TRUNCATE TABLE {table_name};"
        mycursor = mydb.cursor()
        mycursor.execute(query)

    # re-establish foreign keys
    print("Re-establishing Foreign Keys")
    for key_row in foreign_keys:
        query = f"ALTER TABLE {key_row[1]} ADD CONSTRAINT {key_row[0]} FOREIGN KEY ({key_row[2]}) REFERENCES {key_row[3]} ({key_row[4]}) ON UPDATE CASCADE ON DELETE CASCADE"
        mycursor = mydb.cursor()
        mycursor.execute(query)




def prepare_table_import(mydb, table_name, db_connection_info):
    database = db_connection_info["database"]
     # first take the table_name and get all foreign keys
    query = f'''SELECT
    constraint_name,
    table_name,
    column_name,
    referenced_table_name,
    referenced_column_name,
    constraint_schema
    FROM
    information_schema.key_column_usage where TABLE_NAME = '{table_name}' and CONSTRAINT_NAME LIKE '%fk%' and constraint_schema = "{database}";'''

    mycursor = mydb.cursor()

    mycursor.execute(query)
    fk_results = mycursor.fetchall()
    # Get all the table info before making changes
    query = f"DESCRIBE {table_name};"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    table_info = mycursor.fetchall()

    # set all keys to the possibility of null
    fk_info = []
    for row in fk_results:
        column_name = row[2]
        # get the type of the column
        fk_type = [x[1] for x in table_info if x[0] == column_name][0]
        # convert to string
        fk_type = fk_type.decode('utf-8')
        # capitalize
        fk_type = fk_type.upper()
        null = [x[2] for x in table_info if x[0] == column_name][0]
        if null == 'NO':
            null = 'NOT NULL'
        else:
            null = 'NULL'
        fk_info.append([column_name, fk_type, null])
        query = f"ALTER TABLE {table_name} MODIFY {column_name} {fk_type} NULL;"
        mycursor = mydb.cursor()
        mycursor.execute(query)
    return [fk_results, fk_info]


def import_data(db_connection_info, file, table_name, fk_results):
    mydb = mysql.connector.connect(
    host=db_connection_info["host"],
    user=db_connection_info["user"],
    passwd=db_connection_info["passwd"],
    database=db_connection_info["database"],
    allow_local_infile=db_connection_info["allow_local_infile"])



    load_query = f'''LOAD DATA LOCAL INFILE '{file}' REPLACE INTO TABLE {table_name} 
                    FIELDS TERMINATED BY '\\t,;'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 ROWS
                    (filename, filepath, filetype, filesize,fileAtime,fileMtime,fileCtime,points_to)
                    SET listing_path = {os.path.basename(file)};
                    '''

    # execute query
    mycursor = mydb.cursor()
    query = load_query

    mycursor.execute(query)
    mydb.commit()

    

    # Insert the file into listing_paths table
    query = f"INSERT INTO listing_paths (filename, filepath) VALUES ('{os.path.basename(file)}','{file}');"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()

    # Get the ID of the file just inserted
    query = f"SELECT ID FROM listing_paths WHERE filename = '{os.path.basename(file)}' AND filepath = '{file}';"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    myresult = mycursor.fetchall()
    file_ID = myresult[0][0]

    # get index of foreign key that references listing_paths
    i = 0
    for row in fk_results:
        if row[3] == 'listing_paths':
            break
        i += 1
    fk_listing_tuple = fk_results[i]
    

    # Get all the values in the table where the foreign key column is null and set it to the value of the file_ID
    query = f"UPDATE {table_name} SET {fk_listing_tuple[2]} = {file_ID} WHERE {fk_listing_tuple[2]} IS NULL;"
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()

    

def finalize_table_import(mydb, fk_info, table_name):
    # set all keys back to their original values
    for row in fk_info:
        query = f"ALTER TABLE {table_name} MODIFY {row[0]} {row[1]} {row[2]};"
        mycursor = mydb.cursor()
        mycursor.execute(query)

   