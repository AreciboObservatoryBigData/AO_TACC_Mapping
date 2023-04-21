#!/usr/bin/python3.7

import pandas as pd
import os
import sys

included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"

output_listing_dir_path = "Output/src"

# import the included dirs
included_dirs = pd.read_csv(included_dirs_path, sep='\t')

# make new empty dataframe for missing dirs
missing_dirs = pd.DataFrame(columns=['dir_path'])

# loop through the included dirs
for index, row in included_dirs.iterrows():
    output_file_path = os.path.join(output_listing_dir_path, row[0].replace('/', '_')) + ".tsv"
    # if file not found, run listing script
    if not os.path.exists(output_file_path):
        # run listing script
        command = f"./listing.sh {row[0]} {output_file_path}"
        os.system(command)

    # read in the output file, by chunks
    chunksize = 10 ** 6
    for chunk in pd.read_csv(output_file_path, sep='\t', chunksize=chunksize):
        # get only links
        link_chunk = chunk[chunk['filetype'] == 'l'].copy()
        # Get all ponts_to dirs by taking the filename off the end of the path
        link_chunk['points_to_dirs'] = link_chunk['points_to'].apply(lambda x: os.path.dirname(x))
        # Get the distinct points_to values
        points_to = link_chunk['points_to_dirs'].unique()
        # loop through the points_to values
        for point_to in points_to:
            # if the points_to value is not in the included dirs, add it to the missing dirs
            if not included_dirs['dir_path'].str.contains(point_to).any():
                # Add to missing dirs
                missing_dirs = missing_dirs.append(pd.DataFrame({'dir_path': points_to}), ignore_index=True)
        breakpoint()
    



