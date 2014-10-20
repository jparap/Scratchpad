#!/bin/bash
#
# Script to recursively compare all files from one directory to all files in another
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
# Setup variables
path1=$1
path2=$2
path3=$3
logFile=$path3/compareFiles.log
today=`date '+%Y%m%d_%H%M%S'`

# Put marker into log file
echo ">> $today -- file compare started <<" >> $logFile

# Loop through all the files in the first path
# and then compare to the same files in the
# in the second path
for someFile in $(find $path1 -type f)
do
	currentFile=$(basename $someFile) # Strip filename from path
	currentPath=$(dirname $someFile) # Strip path only
	newPath=$(echo $currentPath/ | sed -e "s|$path1|$path2|g") # Swap the first and second paths around
	echo "Checking: $someFile" >> $logFile # Info to log 
	echo "Checking: $someFile" # Info to std out
	# Find the same file in second path maxdepth is just the current directory
	# we dont want to find more than one file at a time
	compareFile=$(find $newPath -maxdepth 1 -name $currentFile)
	if [ -z $compareFile ];then # If file is not found variable will be empty
		# Log warnings and echo to std out if files were not found
		echo "Did not find $currentFile anywhere under path: $newPath" >> $logFile
		echo "Did not find $currentFile anywhere under path: $newPath"
	else
		echo "Compared: $someFile to $compareFile" > $path3/$currentFile.diff
		# Put the diff output details into a single file for each file compare under the
		# file compare path we specified earlier
		diff $someFile $compareFile >> $path3/$currentFile.diff 2>&1
		diff -q $someFile $compareFile >> $logFile 2>&1 # Output a summary to the logfile
	fi
done
