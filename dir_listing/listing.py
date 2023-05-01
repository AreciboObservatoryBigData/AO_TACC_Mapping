#!/usr/bin/python3.7

import pandas as pd
import os
import numpy as np
import glob
import re
import string





included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"
filesystems_info_path = "filesystems_info.tsv"

output_listing_dir_path = "Output/src"


# import the included dirs
included_dirs = pd.read_csv(included_dirs_path, sep='\t')
link_path = "../file_listings/Source_Listing"

blacklist_path = "blacklist.txt"

# import the blacklist
blacklist = []
with open(blacklist_path, "rb") as f:
    blacklist = f.readlines()




def main():
    global included_dirs
    # loop through the included dirs
    for index, row in included_dirs.iterrows():
        output_file_path = os.path.join(output_listing_dir_path, row[0].replace('/', '_')) + ".tsv"
        # if file not found, run listing script
        if not os.path.exists(output_file_path):
            # run listing script
            command = f"./listing.sh {row[0]} {output_file_path}"
            os.system(command)
            
        # Replace all lines that don't decode with the correct line or contain a control character
        # Steps: 
        # Check if you can decode the line, if not,run through process that changes the control characters and the symbols you cant decode
        # If it has any control characters, run through the same process
        
            # open the file
            f = open(output_file_path, "rb")
            i = 0
            replacements = []
            for b_line in f:

                if i ==0:
                    i+=1
                    continue

                # if line cant decode, then run code to add replacement
                # if there's a \t on the filname or filepath, run code to add replacement

                decodeable, make_replacement_bool = check_if_replacement(i,b_line)

                

                if make_replacement_bool == False:
                    i+=1
                    continue
                
                # If make_replacement is true, then get the python filename using glob, then covert each special character to \(int_value of char)
                # split b_line
                split_b_line = b_line.split(b"\t,;")

                # Get all characters in the filepath that can be decoded and not special characters, fill the others with ?* and check with glob
                file_path_pattern_q = ""
                file_path_pattern_w = ""
                for char_int in split_b_line[1]:
                    char = chr(char_int)
                    if char not in string.printable:
                        file_path_pattern_q += "?"
                        file_path_pattern_w += "*"
                        continue

                    file_path_pattern_q += char
                    file_path_pattern_w += char
                # Get actual filepath
                glob_filepath  = glob.glob(file_path_pattern_q)

                # if theres no result, then try with w
                if glob_filepath == []:
                    glob_filepath  = glob.glob(file_path_pattern_w)

                if len(glob_filepath) > 1:
                    breakpoint()
                    print("ERROR! FOUND MORE THAN ONE RESULT FOR THE PATTERN")
                    print(file_path_pattern)
                    print(f"IN FILE: {output_file_path}")
                    print(f"LINE: {i}")

                filepath = glob_filepath[0]


                # replace each special charater with \(int_value)
                new_filepath = "".join([char if char in string.printable else f";({ord(char)})" for char in filepath])

                # change the values in split_b_line
                split_b_line[1] = new_filepath.encode()
                split_b_line[0] = os.path.basename(new_filepath).encode()

                replacement_line = b"\t,;".join(split_b_line)
                replacement_line = replacement_line.decode()

                replacements.append([i+1, replacement_line])

                i+=1





            f.close()
            backup_file_path = output_file_path + ".bak"
            f2 = open(backup_file_path, "w")
            f = open(output_file_path, "rb")
            i = 1
            j = 0
            with open(output_file_path, "rb") as f:
                for line in f:

                    if j < len(replacements):
                        if i == replacements[j][0] :
                            print(replacements[j][1])
                            f2.write(replacements[j][1])
                            j+=1
                        else:
                            f2.write(line.decode())
                    
                    i+=1

            f2.close()

            # remove original file
            os.remove(output_file_path)
            # rename backup file
            os.rename(backup_file_path, output_file_path)



        files = os.listdir(link_path)
        files = [os.path.basename(file) for file in files]
        # Take away finished and readmes
        files.remove("finished")
        files = [file for file in files if not file.endswith(".md")]

        # get all files in finished dir
        finished_files = os.listdir(os.path.join(link_path, "finished"))
        finished_files = [os.path.basename(file) for file in finished_files]

        # extend files with finished files
        files.extend(finished_files)
        
        # create soft link if not already created
        if not os.path.basename(output_file_path) in files:
            command = f"ln -s {os.path.abspath(output_file_path)} {link_path}"
            os.system(command)

        
def check_if_replacement(i, b_line):
    make_replacement_bool = False
    decodable = False
    
    try:
        b_line.decode()
        decodable = True
    except:
        make_replacement_bool = True
    
    # if run still false, check for \t in filename or filepath
    if make_replacement_bool == False:
        # look for the b'\t/' in the line
        line = b_line.decode()
        # get the filepath
        line_split = line.split("\t,;")[1]

        # check for control characters
        has_control_characters_bool = has_control_characters(line)
        make_replacement_bool = has_control_characters_bool

    return decodable, make_replacement_bool


def has_control_characters(input_string):
    # Iterate over each character in the input string
    for char in input_string:
        # Check if the character is not printable
        if char not in string.printable:
            return True
    # If we reach this point, there are no control characters
    return False



main()
