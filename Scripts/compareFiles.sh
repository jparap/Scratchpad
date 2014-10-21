#!/bin/bash
#
# Script to compare all files from one directory to all files in another
# directory. Using a simple diff.
#
# Required: 3 paths, first two for compare directories, 3rd for destination
# directory to place the diff output files

if [ $# -ne 3 ];then
	echo "Compare files in one directory to another"
	echo ""
	echo "Useage: $0 <path1> <path2> <output path>"
	echo ""
	echo "The output file will contain the path and names of the files compared"
	echo "plus the result of the diff command. If no file is found on the second"
	echo "path then this will be recorded here too." 
	exit 1
fi
path1=$1
path2=$2
path3=$3

for someFile in $path1*
do
	currentFile=$(basename $someFile)
	echo "Checking: $currentFile"
	echo "Compared: $path1/$currentFile to $path2/$currentFile" > $path3/$currentFile.diff
	diff $path1/$currentFile $path2/$currentFile >> $path3/$currentFile.diff 2>&1
done
