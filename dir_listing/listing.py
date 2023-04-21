#!/usr/bin/python3.7

import pandas as pd
import os
import sys

included_dirs_path = "included_dirs.tsv"
missing_dirs_path = "missing_dirs.tsv"
broken_dirs_path = "broken_dir_paths.tsv"

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
    chunksize = 10 ** 4
    for chunk in pd.read_csv(output_file_path, sep='\t', chunksize=chunksize):
        # get only links
        link_chunk = chunk[chunk['filetype'] == 'l'].copy()
        # get broken_links
        working_links_bool = link_chunk['points_to'].apply(lambda x: os.path.exists(x))

        # for broken links that have extensions, change the points_to to the dir
        working_links = link_chunk[working_links_bool].copy()

        # change files points_to to dirs
        # get file working_links
        working_links_files_bool = working_links["points_to"].apply(lambda x: os.path.isfile(x))
        # change the points_to to the dir
        working_links.loc[working_links_files_bool,'points_to'] = working_links.loc[working_links_files_bool,'points_to'].apply(lambda x: os.path.dirname(x))

        # get unique values of points_to
        points_to = working_links['points_to'].unique()

        # These values are unique links that work
        # We want to find the dirs that are missing from the included_dirs
        for point in points_to:
            # if the point is not in the included_dirs, add it to the missing_dirs
            if point not in included_dirs['dir_path'].values:
                missing_dirs = missing_dirs.append({'dir_path': point}, ignore_index=True)



    # output missing and broken dirs
    missing_dirs.to_csv(missing_dirs_path, sep='\t', index=False)
    



