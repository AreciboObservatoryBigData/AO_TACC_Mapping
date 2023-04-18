def delete_tables_data(mydb, tables_list):
    # get all foreign keys
    query = '''SELECT
  constraint_name,
  table_name,
  column_name,
  referenced_table_name,
  referenced_column_name
FROM
  information_schema.key_column_usage'''

    mycursor = mydb.cursor()

    mycursor.execute(query)
    myresult = mycursor.fetchall()
    foreign_keys= []
    for row in myresult:
        if 'fk' in row[0]:
            foreign_keys.append(row)
    
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




def import_data(mydb, file, table_name):
    load_query = f'''LOAD DATA LOCAL INFILE '{file}' INTO TABLE Skittles_DB.{table_name} 
                    FIELDS TERMINATED BY '\\t'
                    IGNORE 1 ROWS
                    (filename, filepath, filetype, filesize,fileAtime,fileMtime,fileCtime);
                    '''

    # execute query
    mycursor = mydb.cursor()
    query = load_query
    mycursor.execute(query)
    mydb.commit()


insert_type = "INSERT INTO {dst_table_name} SELECT * FROM {src_table_name} where filetype = '{type}';"

delete_type = "DELETE FROM {table_name} WHERE filetype = '{type}';"

select_dir_names_no_relations = "SELECT ID, filepath FROM {dir_table_name} WHERE ID NOT IN(SELECT DISTINCT {dir_table_name}_ID FROM Skittles_DB.{file_dir_table_name});"

insert_file_dir = "INSERT INTO Skittles_DB.{dst_table_name} SELECT ID, '{dir_ID}' as 'dir_ID' FROM Skittles_DB.{src_table_name} WHERE filepath LIKE '{filepath}%';"
