
import os
import multiprocessing as mp
import time
import string
import pandas as pd
import sys
import subprocess
dirs_path = "included_dirs.tsv"
header = ["filename", "filepath", "filetype", "filesize", "fileAtime", "fileMtime", "fileCtime", "points_to", "dir_name", "broken?"]
separator = "\t,;"
output_file_dir = "Output/src"
count_file_dir = "count.txt"
print_num = 250000
serial = False
map_bool = True
verify = False

# verify = True

char_printable_blacklist = "\n\t\r'\"\x0b\x0c"

allowed_chars = string.printable
for char in char_printable_blacklist:
    allowed_chars = allowed_chars.replace(char, "")




def main():
    global header
    global separator

    # Read in lines from dirs_path
    dirs = []
    with open(dirs_path, "r") as dirs_file:
        for line in dirs_file:
            if line.strip() == "":
                continue
            dirs.append(line.strip())
    # Remove header
    dirs = dirs[1:]

    for dir_path in dirs:
        print("Staring directory: " + dir_path)
        os.system("ls "+ dir_path + " > /dev/null")
        output_file_path = os.path.join(output_file_dir, dir_path.replace("/", "_,:_") + ".txt")
        if os.path.exists(output_file_path):
            print("Output file already exists: " + output_file_path)
            continue

        # Open output file
        output_file = open(output_file_path, "w")
        # Write header
        output_file.write(separator.join(header) + "\n")

        # write in dir_path results
        filepath, filetype, line = getLine(dir_path)
        output_file.write(line + "\n")


        pool = mp.Pool(processes=200)

        # Get file count
        command = "find " + dir_path + " | wc -l > " + count_file_dir
        
        # file_count_p = pool.apply_async(os.system, args=(command,))
        # breakpoint()

        # Get Listing
        listing = os.listdir(dir_path)
        # Make absolute paths
        listing = [os.path.join(dir_path, item) for item in listing]

        tasks = listing[:]
        i = 1
        # Keep track of resolved path of the ld and the points_to of the ld
        # if points_to has already been scanned, check if the resolved_path is a subpath of the current path
        # if it is, then it is a looping link
        scanned_ld_paths = []
        scanned_ld_points_to = []
        finished_dirs = []
        while len(tasks) > 0:
            
            # TO run in serial:
            ###########################
            if serial:
                filepath, filetype, line = getLine(tasks[0])
                tasks.pop(0)

                if line == "":
                    continue
                output_file.write(line + "\n")
                # get broken? index from header
                broken_index = header.index("points_to")
                # Break line by separator and get broken?
                points_to = line.split(separator)[broken_index]


                if (filetype == "d" or filetype == "ld") and points_to not in finished_dirs:
                    loop_bool = False
                    if filetype == "ld":
                        
                        points_to = line.split(separator)[header.index("points_to")]
                        loop_bool = loopCheck(filepath, points_to, scanned_ld_paths, scanned_ld_points_to)
                        # if its not a loop, then add the points_to and filepath to the scanned lists
                        if not loop_bool:
                            scanned_ld_paths.append(filepath)
                            scanned_ld_points_to.append(points_to)
                        # print(f"points_to:\n{scanned_ld_points_to}")
                        # print(f"filepath:\n{scanned_ld_paths}")
                        # breakpoint()
                    
                    # Add to tasks if not a loop and if, when the filetype is ld, the broken? is not broken
                    if not loop_bool:
                        try:
                            listing = os.listdir(filepath)
                            listing = [os.path.join(filepath, item) for item in listing]
                            tasks.extend(listing)
                        except:
                            print("Error with: " + filepath)

                if i % print_num == 0:
                    print("Completed " + str(i) + " tasks")
                i += 1
            ###########################
            else:
                print("Submitting " + str(len(tasks)) + " tasks")
                # for task in tasks:
                #     print(task)
                try:
                    results = pool.map(getLine, tasks)
                except:
                    import pdb; pdb.set_trace()
                finished_dirs.extend(tasks)
                tasks = []

                for result in results:
                    filepath, filetype, line = result
                    
                    
                        

                    if line == "":
                        continue
                    output_file.write(line + "\n")
                    # get broken? index from header
                    broken_index = header.index("points_to")
                    # Break line by separator and get broken?
                    points_to = line.split(separator)[broken_index]


                    if (filetype == "d" or filetype == "ld") and points_to not in finished_dirs:
                        loop_bool = False
                        if filetype == "ld":
                            
                            points_to = line.split(separator)[header.index("points_to")]
                            loop_bool = loopCheck(filepath, points_to, scanned_ld_paths, scanned_ld_points_to)
                            # if its not a loop, then add the points_to and filepath to the scanned lists
                            if not loop_bool:
                                scanned_ld_paths.append(filepath)
                                scanned_ld_points_to.append(points_to)
                            # print(f"points_to:\n{scanned_ld_points_to}")
                            # print(f"filepath:\n{scanned_ld_paths}")
                            # breakpoint()
                        
                        # Add to tasks if not a loop and if, when the filetype is ld, the broken? is not broken
                        if not loop_bool:
                            try:
                                listing = os.listdir(filepath)
                                listing = [os.path.join(filepath, item) for item in listing]
                                tasks.extend(listing)
                            except:
                                print("Error with: " + filepath)

                    if i % print_num == 0:
                        print("Completed " + str(i) + " tasks")
                    i += 1
        # file_count_p.join()
        pool.close()
        # print("Find File Count")

        print(i)
                
                    
            
        output_file.close()
        
        if verify:
            verifyOutput(output_file_path, dir_path)



def getLine(filepath):
    
    global header
    global separator
    global allowed_chars
    # make dictionary slowly
    dict_line = {}

    # set default value for broken?
    dict_line["broken?"] = ""


    
    # only allow characters in allowed_chars
    dict_line["filepath"] = ""
    for char in filepath:
        dict_line["filepath"] += getIfAllowableChar(char)
    

    dict_line["filename"] = os.path.basename(dict_line["filepath"])
    dict_line["filetype"] = getFileType(filepath)
    if dict_line["filetype"] == "d":
        try:
            listing = os.listdir(filepath)
        except  OSError as e:
            if e.errno == 13:
                
                print("Permission denied to access the directory:", filepath)
                return filepath, dict_line["filetype"], ""
            else:
                print("Error accessing the directory:", filepath)
                sys.exit(1)
    elif dict_line["filetype"] == "?":
        # Check if I have the permissions to get info about the file or directory
        try:
            os.stat(filepath)
        except  OSError as e:
            if e.errno == 13:
                print("Permission denied to access the directory/file:", filepath)
                return filepath, "?", ""
            else:
                print("Error accessing the directory:", filepath)
                sys.exit(1)
        
    if dict_line["filetype"] == "l":
        link_info = os.lstat(filepath)
        
        dict_line["filesize"] = link_info.st_size
        dict_line["points_to"] = os.readlink(filepath)
        # Check if its broken
        dict_line["broken?"] = check_link(dict_line["points_to"])
        
        # Now that we know where it points to, check if points to file or directory and assign lf ld or ll, depending on which
        # if os.path.
        # If points_to is a relative path, make it absolute
        if dict_line["points_to"][0] != "/":
            dir_name = os.path.dirname(filepath)

            dict_line["points_to"] = relToAbs(dir_name, dict_line["points_to"])

        # Here the link points_to is in absolute path form
        if os.path.isfile(dict_line["points_to"]):
            dict_line["filetype"] = "lf"
        elif os.path.isdir(dict_line["points_to"]):
            dict_line["filetype"] = "ld"
        elif os.path.isdir(dict_line["points_to"]):
            dict_line["filetype"] = "ll"
        points_to_filepath = dict_line["points_to"]
        dict_line["points_to"] = ""
        for char in points_to_filepath:
            dict_line["points_to"] += getIfAllowableChar(char)
        dict_line["fileAtime"] = link_info.st_atime
        dict_line["fileMtime"] = link_info.st_mtime
        dict_line["fileCtime"] = link_info.st_ctime

    else:
        dict_line["filesize"] = os.path.getsize(filepath)
        dict_line["points_to"] = ""
        dict_line["fileAtime"] = os.path.getatime(filepath)
        dict_line["fileMtime"] = os.path.getmtime(filepath)
        dict_line["fileCtime"] = os.path.getctime(filepath)
    
    # Add base_dir
    dict_line["dir_name"] = os.path.dirname(dict_line["filepath"])
    
    # make line
    line = ""
    line += str(dict_line[header[0]])
    for key in header[1:]:
        line += separator +  str(dict_line[key]) 
    
    # Remove last
    return filepath, dict_line["filetype"], line

def getIfAllowableChar(char):
    if char in allowed_chars:
        return char
    else:
        return ";(" + str(ord(char)) + ");"   

def getFileType(filepath):
    if os.path.islink(filepath):
        return "l"
    elif os.path.isdir(filepath):
        return "d"
    elif os.path.isfile(filepath):
        return "f"
    else:
        # print("Unknown file type for file: " + filepath)
        return "?"

def verifyOutput(output_file_path, dir_path):
    global header
    global separator
    find_file_path = output_file_path + ".find"
    # Run find on dir_path
    command = "find " + dir_path + " > " + find_file_path
    os.system(command)

    # open find file
    find_file = open(find_file_path, "r")
    for line in find_file:
        # Search for line in output_file
        line = line.strip()
        # Search for line in output_file
        output_file = open(output_file_path, "r")
        found = False
        for output_line in output_file:
            output_line = output_line.strip()
            output_line_split = output_line.split(separator)

            if line == output_line_split[1]:
                found = True
                break
        output_file.close()
        if not found:
            print("Could not find line: " + line)
            sys.exit(1)

def loopCheck(filepath, points_to, scanned_ld_paths, scanned_ld_points_to):
    
    # #  Check if filepath is in scanned_ld_points_to
    if points_to in scanned_ld_points_to:
        # If it is, then check if any element in scanned_ld_paths is a substring of filepath
        for scanned_ld_path in scanned_ld_paths:
            if scanned_ld_path in filepath:
                return True
    return False

def check_link(link_path):
    
    command = "test -e '" + link_path + "'"
    result = subprocess.call(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result == 0:
        return 0
    else:
        return 1

def relToAbs(dir_name, rel_path):
    split_rel_path = rel_path.split("/")
    split_dir_name = dir_name.split("/")

    for i, rel in enumerate(split_rel_path):
        if rel == "..":
            split_dir_name.pop()
        elif rel == ".":
            continue
        else:
            split_dir_name.append(rel)
    abs_path = "/".join(split_dir_name)
    return abs_path



main()