#!/usr/bin/python3.7

import pandas as pd
import os
import numpy as np
import subprocess
import math
import sys

included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"
filesystems_info_path = "filesystems_info.tsv"

output_listing_dir_path = "Output/src"

missing_dirs_black_list = [
    "/proj"
]

# import the included dirs
included_dirs = pd.read_csv(included_dirs_path, sep='\t')
# import the filesystems info
filesystems_df = pd.read_csv(filesystems_info_path, sep='\t')

# make new empty dataframe for missing dirs
missing_dirs = pd.DataFrame(columns=['dir_path'])

# make new empty dataframe for broken links
broken_links_total = pd.DataFrame(columns=['filepath', 'filetype', 'points_to'])



def main():
    global included_dirs
    global missing_dirs
    global broken_links_total
    # loop through the included dirs
    for index, row in included_dirs.iterrows():
        output_file_path = os.path.join(output_listing_dir_path, row[0].replace('/', '_')) + ".tsv"
        # if file not found, run listing script
        if not os.path.exists(output_file_path):
            # run listing script
            command = f"./listing.sh {row[0]} {output_file_path}"
            os.system(command)
        print(f"Finished listing {output_file_path}")
        


main()
