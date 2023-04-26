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

        # create soft link if not already created
        if not os.path.basename(output_file_path) in files:
            command = f"ln -s {os.path.abspath(output_file_path)} {link_path}"
            os.system(command)
        
        


main()
