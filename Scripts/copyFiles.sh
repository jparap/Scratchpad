#!/bin/bash

#
# Script to find logs and copy them to current dir with IP prefix
# for use with a OpsCenter diagnostic output
#
# Required: 1 path, to find the log files
# Required: 1 log file name to search for

if [ $# -ne 2 ];then
	echo "Find files and copy to current dir with IP prefix"
	echo ""
	echo "Useage: $0 <path> <file name>"
	echo ""
	echo "Note: run above the nodes directory"
	echo ""
	exit 1
fi

# 1. Find all files and copy into local dir

# Setup local variables
path1=$1
myFile=$2

# function to find given files and copy to local dir with the node ip prefix 
function findFiles {
for someFile in $(find $path1 -name $myFile)
do
	currentFileName=$(basename $someFile)
	currentPathName=$(dirname $someFile)
	newPathName=$(echo $currentPathName | awk -F "nodes/" '{print $2}' | awk -F\/ '{print $1}'| awk -F\- '{print $(NF)}') # Strip ip from path
	newFileName="$newPathName-$currentFileName"
	echo "Copying $someFile to $newFileName"
	cp $someFile $newFileName # Copy the file to the new filename
done
}

# Call the function
findFiles
