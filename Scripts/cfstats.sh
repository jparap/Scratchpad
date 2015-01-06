#!/bin/bash

#
# Script to find cfstats files and collate sstable info for each node
#
# Required: 1 path, to find the cfstats files
# Required: 1 keyspace, to find in the cfstats files

if [ $# -ne 2 ];then
	echo "Find cfstats files and collate"
	echo ""
	echo "Useage: $0 <path> <keyspace>"
	echo ""
	exit 1
fi

# 1. Find all cfstats files and copy into local dir
# 2. Find all table names in given keyspace
# 3. For each table check each cfstats file for all parameters

# Setup local variables
path1=$1
myFile=cfstats
myDir=workingFiles
myKeyspace=$2

# create local workingFiles dir
if [ ! -d ./$myDir ]
then
	mkdir -p ./$myDir
fi

# function to find given files and copy to local dir with the node ip prefix 
function findFiles {
for someFile in $(find $path1 -name $myFile)
do
	currentFile=$(basename $someFile) # Strip filename from path
	currentPath=$(dirname $someFile) # Strip path only
	nodeIp=$(echo $currentPath | awk -F\- '{print $(NF)}'| awk -F\/ '{print $(NF-1)}') # Strip ip from path
	newFileName="$myDir/$nodeIp-$currentFile" # make a new file name with the IP
	cp $someFile $newFileName # Copy the cfstats file to the new filename
	echo "Copying $someFile to $newFileName"
done
}

# function to find all table names in a given keyspace
function findTables {
for someFile in $(find ./$myDir -name "*cfstats")
do
	i=$((i+1))
	tableName=./$myDir/tables$i
	# Note using single quotes prevents variable expansion this had me going for a while!
	#tableName=$(sed -n '/^Keyspace: $myKeyspace/,/^Keyspace/p' $someFile | grep "Table:")
	sed -n "/^Keyspace: $myKeyspace/,/^Keyspace/p" $someFile | grep "Table:" > $tableName # Get all tables in the Keyspace
done
# Cat all resulting files and then sort uniquely to get all the tables. The order isnt really important
for someFile in $(find ./$myDir -name "tables*")
do
	cat $someFile >> $myDir/allTables
done
cat $myDir/allTables | sort -u > $myDir/uniqueTables
}

# function to pull out all stats from a given table
function pullStats {
myValue=$1
while read someTable 
do
	echo -e "\n$someTable"
	grep -A21 -w "$someTable" ./$myDir/*cfstats | grep -w $myValue | awk -F\: '{print $1,$2,$3}'| awk -F"/" '{print $3}'| awk -F"-" '{print $1,$3}'
	grep -A21 -w "$someTable" ./$myDir/*cfstats | grep -w $myValue | awk -F\: '{total += $2} END {print "TOTAL",myValue,total}'
done < ./$myDir/uniqueTables
}

# function to swap out the "space" char in a string for [[:space:]]
function format {
myString="$1"
myNewVal=$(echo $myString | sed -e 's/\ /[[:space:]]/g')
}

# Find the files and copy to local
findFiles
# Find the tables in the given keyspace
findTables 
# Find all the following values, comment out the ones you dont need
#
# So whats with the [[:space:]]? - these are so grep actually looks
# for the space character, other wise "one two" will match both "one"
# and "two" which isn't the same as when you run in on the command line
#


format "SSTable count:"
pullStats $myNewVal

format "Space used (live), bytes"
pullStats $myNewVal

format "Space used (total), bytes:"
pullStats $myNewVal

format "SSTable Compression Ratio:"
pullStats $myNewVal

format "Number of keys (estimate):"
pullStats $myNewVal
 
format "Memtable cell count:"
pullStats $myNewVal

format "Memtable data size, bytes:"
pullStats $myNewVal

format "Memtable switch count:"
pullStats $myNewVal

format "Local read count:"
pullStats $myNewVal

format "Local read latency:"
pullStats $myNewVal

format "Local write count:"
pullStats $myNewVal

format "Local write latency:"
pullStats $myNewVal

format "Pending tasks:"
pullStats $myNewVal

format "Bloom filter false positives:"
pullStats $myNewVal

format "Bloom filter false ratio:"
pullStats $myNewVal

format "Bloom filter space used, bytes:"
pullStats $myNewVal

format "Compacted partition minimum bytes:"
pullStats $myNewVal

format "Compacted partition maximum bytes:"
pullStats $myNewVal

format "Compacted partition mean bytes:"
pullStats $myNewVal

format "Average live cells per slice (last five minutes):"
pullStats $myNewVal

format "Average tombstones per slice (last five minutes):"
pullStats $myNewVal

exit 

