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
num_cores = 100
def main():
    print("Starting")
    global f
    chunksize = 10 ** 5

    # read link_info_path line by line
    # Make pool
    pool = mp.Pool(num_cores)
    processes = []
    i = 0
    for line in open(link_info_path, "r"):

        if i == 0:
            f.write(line)
            i += 1
            continue
        if line[-1] == "\n":
            line = line[:-1]
        
        split_line = line.split("\t")
        points_to = split_line[1]
        processes.append(pool.apply_async(check_link, args=(points_to,)))
        i += 1
    print("All processes started")
    print("Processes: ", len(processes))
    # Re-open file to re-read
    f2 = open(link_info_path, "r")
    f2.readline()
    # Now wait for the processes to finish and get the results
    for process in processes:
        result = process.get()
        line = f2.readline()
        if line[-1] == "\n":
            line = line[:-1]
        split_line = line.split("\t")
        split_line[-1] = str(result)
        line = "\t".join(split_line)
        f.write(line + "\n")
    pool.close()
    pool.join()
    print("Pool closed")
    print("Broken links written to file")

    


    # link_info_df = pd.read_csv(os.path.abspath(link_info_path), sep='\t', chunksize=chunksize)
    # # write same header as the first line of the original file
    # with open(link_info_path, "r") as f2:
    #     line = f2.readline()
    #     f.write(line)
    # i = 0
    # for chunk in link_info_df:
    #     print(f"Chunk {i} started")
        
    #     split_tasks = np.array_split(chunk["points_to"], num_cores)
    #     pool = mp.Pool(num_cores)
    #     results = pool.map(check_link_apply, split_tasks)
    #     print("Pool closed")
    #     pool.close()
    #     result_df = pd.concat(results)
    #     chunk["broken_link"] = result_df
    #     print("Broken links written to file")
    #     chunk.apply(chunk_to_file, axis=1)

    #     i+=1


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
