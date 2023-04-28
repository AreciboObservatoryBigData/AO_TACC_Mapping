#!/usr/bin/python3.7

import pandas as pd
import os
import numpy as np
import glob
import re




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
            # open the file
            f = open(output_file_path, "rb")
            i = 0
            replacements = []
            for line in f:
                

                try:
                    line.decode()
                except:
                    # get the filepath pattern
                    split_dir_bytes = line[line.index(b"\t/")+1:].split(b"/")[0:-1]
                    split_dir = [element.decode() for element in split_dir_bytes]
                    dir_path = "/".join(split_dir)
                    filename = line[:line.index(b"\t/")]
                    files = os.listdir(dir_path)
                    listing_filename = ""
                    filtered_files = files.copy()
                    i2 = 0
                    for char_int in filename:
                        try:
                            filtered_files = [element for element in filtered_files if filename[i2:i2+1].decode() == element[i2]]
                        except:
                            i2+=1
                            continue
                        if len(filtered_files) == 1:
                            listing_filename = filtered_files[0]
                            break
                        i2+=1
                    file_path = os.path.join(dir_path, listing_filename)

                    # get index where we find \t and 4 numbers
                    pattern = b"\t\d{4}"
                    match = re.search(pattern, line)
                    rest_of_line = line[match.start()+1:]
                    replacement_line = listing_filename + "\t" + file_path + "\t" + rest_of_line.decode()

                    # Change filepath special \ to their hex representation except for \t and \n
                    new_replacement_line = ""
                    for i2, char in enumerate(replacement_line):
                        try:
                            new_replacement_line+=replacement_line[i2:i2+1].encode().decode()

                        except:
                            new_replacement_line+=f"chr({ord(char)})"
                    
                    # replace all control characters with their integer representation, except for \t and \n

                    pattern = r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\xFF]'
                    new_replacement_line = re.sub(pattern, lambda x: f"chr({ord(x.group(0))})", new_replacement_line)

                    replacements.append([i+1, new_replacement_line])
                
                i+=1


            f.close()
            backup_file_path = output_file_path + ".bak"
            f2 = open(backup_file_path, "w")
            f = open(output_file_path, "rb")
            i = 1
            j = 0
            with open(output_file_path, "rb") as f:
                for line in f:
                    try:
                        if j < len(replacements):
                            if i == replacements[j][0] :
                                f2.write(replacements[j][1])
                                j+=1
                            else:
                                f2.write(line.decode())
                    except:
                        breakpoint()
                    i+=1

            f2.close()

            # remove original file
            os.remove(output_file_path)
            # rename backup file
            os.rename(backup_file_path, output_file_path)



        files = os.listdir(link_path)
        files = [os.basename(file) for file in files]
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

        
        


main()
