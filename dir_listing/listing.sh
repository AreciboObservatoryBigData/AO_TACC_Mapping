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

# Create output directory
# mkdir -p $OUTPUT

# get basename of directory
# DIR_BASENAME=$(basename $OUTPUT)


# three files will be made, with the output_directory specified
# files_output="$OUTPUT/$DIR_BASENAME"_files.tsv
# dirs_output="$OUTPUT/$DIR_BASENAME"_dirs.tsv
# symlinks_output="$OUTPUT/$DIR_BASENAME"_symlinks.tsv

# echo $files_output
# echo $dirs_output
# echo $symlinks_output


echo "Starting Directory: $DIR"

# Log start time, date, and final command
echo "Start time: $(date)" >> run_log.log
echo "Command: find $DIR -type f -printf %f\t%p\t%y\t%s\t%A@\t%T@\t%C@\n >> $files_output" >> run_log.log

# Create output tsv file
touch $OUTPUT

# Add header to tsv file
echo -e "fileName\tfilePath\tfileType\tfileSize\tfileAtime\tfileMtime\tfileCtime" >> $OUTPUT

# Use:
# find {dir_path} -type f -exec stat --printf='\"%n\"\t%s\t%W\t%X\t%Y\t%Z\n' {} + >> {results_path}
# to get the file name, size, last access time, last modification time, last status change time, and creation time
find $DIR -type f -printf "%f\t%p\t%y\t%s\t%A@\t%T@\t%C@\n" >> $files_output

echo "Finished files in Directory: $DIR"

# Log start time, date, and final command
echo "Start time: $(date)" >> run_log.log
echo "Command: find $DIR -type d -printf %f\t%p\t%y\t%s\t%A@\t%T@\t%C@\n >> $dirs_output" >> run_log.log

# Create output tsv file
touch $dirs_output

# Add header to tsv file
echo -e "fileName\tfilePath\tfileType\tfileSize\tfileAtime\tfileMtime\tfileCtime" >> $dirs_output

# Use:
# find {dir_path} -type f -exec stat --printf='\"%n\"\t%s\t%W\t%X\t%Y\t%Z\n' {} + >> {results_path}
# to get the file name, size, last access time, last modification time, last status change time, and creation time
find $DIR -type d -printf "%f\t%p\t%y\t%s\t%A@\t%T@\t%C@\n" >> $dirs_output

echo "Finished directories in Directory: $DIR"

# Log start time, date, and final command
echo "Start time: $(date)" >> run_log.log
echo "Command: find -type l -printf %f\t%p\t%y\t%s\t%A@\t%T@\t%C@\t -exec sh -c 'readlink -f \"\$0\"' {} \; -printf \n" >> run_log.log

# Create output tsv file
touch $symlinks_output

# Add header to tsv file
echo -e "fileName\tfilePath\tfileType\tfileSize\tfileAtime\tfileMtime\tfileCtime\tpoints_to" >> $symlinks_output

# Use:
# find {dir_path} -type f -exec stat --printf='\"%n\"\t%s\t%W\t%X\t%Y\t%Z\n' {} + >> {results_path}
# to get the file name, size, last access time, last modification time, last status change time, and creation time
find $DIR -type l -printf "%f\t%p\t%y\t%s\t%A@\t%T@\t%C@\t" -exec sh -c 'readlink -f "$0"' {} \; -printf "\n" >> $symlinks_output

echo "Finished symlinks in Directory: $DIR"

echo "Finished Directory: $DIR"

# Log end time and date
echo "End time: $(date)" >> run_log.log

