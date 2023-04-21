#!/usr/bin/python3.7

import pandas as pd
import os
import sys

included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"

output_listing_dir_path = "Output"

# import the included dirs
included_dirs = pd.read_csv(included_dirs_path, sep='\t')

# make new empty dataframe for missing dirs
missing_dirs = pd.DataFrame(columns=['dir_path'])

# loop through the included dirs
for index, row in included_dirs.iterrows():
    # run listing script
    command = f"./listing.sh {row[0]} {os.path.join(output_listing_dir_path, row[0].replace('/', '_'))}.tsv"
    os.system(command)
    


