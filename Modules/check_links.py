import pandas as pd
import os
import subprocess
import multiprocessing as mp
import numpy as np

general_files_path = "../general_files"
link_info_path = os.path.join(general_files_path, "link_info.tsv")
new_link_info_path = os.path.join(general_files_path, "new_link_info.tsv")
f = open(new_link_info_path, "w")

# Define the number of cores to use
num_cores = mp.cpu_count() - 4
def main():
    global f
    chunksize = 10 ** 5

    link_info_df = pd.read_csv(os.path.abspath(link_info_path), sep='\t', chunksize=chunksize)
    # write same header as the first line of the original file
    with open(link_info_path, "r") as f2:
        line = f2.readline()
        f.write(line)
    for chunk in link_info_df:
        
        split_tasks = np.array_split(chunk["points_to"], num_cores)
        pool = mp.Pool(num_cores)
        results = pool.map(check_link_apply, split_tasks)
        pool.close()
        result_df = pd.concat(results)
        chunk["broken_link"] = result_df
        chunk.apply(chunk_to_file, axis=1)


def check_link_apply(row):
    return row.apply(check_link)

def check_link(link_path):
    
    command = f"test -e {link_path}"
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode == 0:
        return 0
    else:
        return 1

def chunk_to_file(row):
    global f
    line = ""
    for index, value in row.iteritems():
        line += str(value) + "\t"
    line = line[:-1] + "\n"
    f.write(line)
    

main()
