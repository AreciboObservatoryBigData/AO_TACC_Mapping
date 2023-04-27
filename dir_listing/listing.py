#!/usr/bin/python3.7

import pandas as pd
import os
import numpy as np
import glob




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
        print(f"Finished listing {output_file_path}")

        # get all files in the directory
        files = glob.glob(os.path.join(link_path, "*"))
        # make all of them basenames
        files = [os.path.basename(file) for file in files]
        # get finished directory contents
        files.remove("finished")
        files.extend(glob.glob(os.path.join(link_path, "finished", "*")))
        files = [os.path.basename(file) for file in files]

        
        # Remove all lines that are found in the blacklist
        encoded_included_dir = row[0].encode()
        blacklist_included_dir = [element for element in blacklist if encoded_included_dir in element]
        if blacklist_included_dir != []:
            with open(output_file_path, "rb") as f:
                lines_to_remove = []
                i = 1
                for line in f:
                    if line in blacklist_included_dir:
                        lines_to_remove.append(i)
                        blacklist_included_dir.remove(line)
                    i += 1
        # for each lines to remove, remove the line from the file using sed
        for line in lines_to_remove:
            command = f"sed -i '{line}d' {output_file_path}"
            os.system(command)
        # create soft link if not already created
        if not os.path.basename(output_file_path) in files:
            command = f"ln -s {os.path.abspath(output_file_path)} {link_path}"
            os.system(command)

        
        


main()
