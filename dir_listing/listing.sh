#!/bin/bash

# This script will list all the files in the user defined directory

# Get first positional parameter
DIR=$1

# if no positional parameter is given error out
if [ -z "$DIR" ]; then
    echo "No directory specified"
    exit 1
fi

# Second positional parameter is the output filepath
OUTPUT=$2

# if no output file is specified, error out
if [ -z "$OUTPUT" ]; then
    echo "No output file specified"
    exit 1
fi

# If output file already exists, remove it
if [ -f "$OUTPUT" ]; then
    rm $OUTPUT
fi



echo "Starting Directory: $DIR"

# Log start time, date, and final command
echo "Start time: $(date)" >> run_log.log
echo "Command: find $DIR -type f -printf %f\t%p\t%y\t%s\t%A@\t%T@\t%C@\n >> $OUTPUT" >> run_log.log

# Create output tsv file
touch $OUTPUT

# Add header to tsv file
echo -e "filename\tfilepath\tfiletype\tfilesize\tfileAtime\tfileMtime\tfileCtime\tpoints_to" >> $OUTPUT



# Log start time, date, and final command
echo "Start time: $(date)" >> run_log.log
echo "Command: find $DIR -printf %f\t,;%p\t,;%y\t,;%s\t,;%A@\t,;%T@\t,;%C@\t,; -exec sh -c 'readlink -f \"\$0\"' {} \; -printf \n" >> run_log.log


# Use:
# find {dir_path} -type f -exec stat --printf='\"%n\"\t%s\t%W\t%X\t%Y\t%Z\n' {} + >> {results_path}
# to get the file name, size, last access time, last modification time, last status change time, and creation time
find $DIR -printf "%f\t,;%p\t,;%y\t,;%s\t,;%A@\t,;%T@\t,;%C@\t,;" -exec bash -c 'readlink "$0" 2>error.log || echo -n' {} \; -printf "\n" | sed '/^$/d' >> $OUTPUT





echo "Finished Directory: $DIR"


# Log end time and date
echo "End time: $(date)" >> run_log.log

