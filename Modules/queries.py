delete_table_data = "DELETE FROM Skittles_DB.{table_name};"

import_data = "LOAD DATA LOCAL INFILE '{file}'INTO TABLE Skittles_DB.{table_name} FIELDS TERMINATED BY '\\t'IGNORE 1 ROWS;"

insert_type = "INSERT INTO {dst_table_name} SELECT * FROM {src_table_name} where filetype = '{type}';"

delete_type = "DELETE FROM {table_name} WHERE filetype = '{type}';"

select_path_lists = "SELECT filepath FROM {table_name};"

insert_file_dir = "INSERT INTO Skittles_DB.{dst_table_name} SELECT filepath, '{filepath}' as 'dir_filepath' FROM Skittles_DB.{src_table_name} WHERE filepath LIKE '{filepath}%'"
