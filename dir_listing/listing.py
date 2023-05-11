#!/usr/bin/python3.7

import pandas as pd
import os
import numpy as np
import glob
import re
import string





included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"

output_listing_dir_path = "Output/src"


# import the included dirs
included_dirs = pd.read_csv(included_dirs_path, sep='\t')
link_path = "../file_listings/Source_Listing"

blacklist_path = "blacklist.txt"

# control variables
lines_per_split_file = 250000
run_find = True
replace_special_characters = True
split_files_bool = True
add_links_bool = True
leave_original_files = True



def main():
    global included_dirs
    # loop through the included dirs
    for index, row in included_dirs.iterrows():
        output_file_path = os.path.join(output_listing_dir_path, row[0].replace('/', '_')) + ".txt"
        # Get what the oputput_dir_path would be
        output_dir_path = os.path.join(output_listing_dir_path, row[0].replace('/', '_'))

        


        # if run_find is true, then run the find command
        if run_find == True and not os.path.exists(output_dir_path):
            # run listing script
            # command = f"./listing.sh {row[0]} {output_file_path}"
            command = "./listing.sh " + row[0] + " " + output_file_path
            os.system(command)
        

        if replace_special_characters and not os.path.exists(output_file_path) and run_find == True:
            print("Replacing special characters in file: " + output_file_path)
            # Replace all lines that don't decode with the correct line or contain a control character
            # Steps: 
            # Check if you can decode the line, if not,run through process that changes the control characters and the symbols you cant decode
            # If it has any control characters, run through the same process
        
            # open the file
            f = open(output_file_path, "rb")
            i = 0
            replacements = []
            for b_line in f:
                # skip first line
                if i ==0:
                    i+=1
                    continue

                # if line cant decode, then run code to add replacement
                # if there's a \t on the filename or filepath, run code to add replacement

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
                    print("ERROR! FOUND MORE THAN ONE RESULT FOR THE PATTERN")
                    print(file_path_pattern_q)
                    print(file_path_pattern_w)
                    print("IN FILE: "+output_file_path)
                    print("LINE: "+str(i))
                    # exit with error
                    exit(1)

                filepath = glob_filepath[0]


                # replace each special charater with \(int_value)

                new_filepath = "".join([char if char in string.printable else ";("+str(ord(char))+")" for char in filepath ])

                # do the same for \r
                new_filepath = new_filepath.replace("\r", ";(" + str(ord("\r")) + ")")

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
                            # print(replacements[j][1])
                            f2.write(replacements[j][1])
                            j+=1
                            i+=1
                            continue
                        
                    f2.write(line.decode())
                    
                    i+=1
                    

            f2.close()

            if not leave_original_files:
                # remove original file
                os.remove(output_file_path)
                # rename backup file
                os.rename(backup_file_path, output_file_path)
            else:
                # rename original file to .orig
                os.rename(output_file_path, output_file_path + ".orig")
                # rename backup file to original file
                os.rename(backup_file_path, output_file_path)

        if split_files_bool:
            print("Splitting file: " + output_file_path)
            split_file(output_file_path, lines_per_split_file)

        if add_links_bool:
            add_links(output_file_path)


        
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
        if char == "\r":
            return True
    # If we reach this point, there are no control characters
    return False

def split_file(listing_file_path, lines_per_file):
    global leave_original_files
    
    filename = os.path.basename(listing_file_path)
    
    dirpath = os.path.join(os.path.dirname(listing_file_path), filename.split('.')[0])

    # if directory does not exist, create it
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
    
    # remove all files in dirpath
    files = glob.glob(os.path.join(dirpath, "*"))
    for file in files:
        os.remove(file)
    
    with open(listing_file_path, 'r') as f:
            # get header
            header = f.readline()
            i = 1
            file_i = 0
            # get output_file_path
            output_file_path = os.path.join(dirpath, filename.split('.')[0] + '_' + str(file_i) + '.txt')
            # open output file as write
            output_file = open(output_file_path, 'w')
            # write header to output file
            output_file.write(header)
            for line in f:
                
                if i > lines_per_file:
                    print(i + (file_i * lines_per_file))
                    # Reset i
                    i = 1
                    # add 1 to file_i
                    file_i += 1
                    # change file to write to
                    output_file_path = os.path.join(dirpath, filename.split('.')[0] + '_' + str(file_i) + '.txt')
                    # close output file
                    output_file.close()
                    # open output file as write
                    output_file = open(output_file_path, 'w')
                    # write header to output file
                    output_file.write(header)
                    
                # write line to output file
                output_file.write(line)

                i +=1
            
            print(i + (file_i * lines_per_file))
            # close output file
            output_file.close()
    if not leave_original_files:
        # remove original file
        os.remove(listing_file_path)

def add_links(output_file_path):
    global link_path
    # Get all files created by split_file
    # Get the directory created by split_file
    split_file_output_dir = os.path.join(os.path.dirname(output_file_path), os.path.basename(output_file_path).split('.')[0])

    # check if the directory has been made in the links_path
    links_path_split_file_dir = os.path.join(link_path, os.path.basename(output_file_path).split('.')[0])
    # if exists, remove it
    if os.path.exists(links_path_split_file_dir):
        command = "rm -r " + links_path_split_file_dir
        os.system(command)
    
    # check in the finished directory
    links_path_split_file_finished_dir = os.path.join(link_path, "finished", os.path.basename(output_file_path).split('.')[0])
    # if exists, remove it
    if os.path.exists(links_path_split_file_finished_dir):
        os.rmdir(links_path_split_file_finished_dir)
    
    # create the directory in the links_path
    os.mkdir(links_path_split_file_dir)

    # Get all files in the split_file_output_dir
    split_file_output_files = glob.glob(os.path.join(split_file_output_dir, "*"))

    # Change the items to absolute paths
    split_file_output_files = [os.path.abspath(file) for file in split_file_output_files]

    # Create the links in the links_path_split_file_dir
    for file in split_file_output_files:
        os.symlink(file, os.path.join(links_path_split_file_dir, os.path.basename(file)))


main()
