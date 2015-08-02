#!/bin/bash

#
# Script to find cfstats files and collate sstable info for each node
#
# Required: 1 path, to find the cfstats files
# Required: 1 keyspace, to find in the cfstats files
if [ $# -ne 3 ];then
	echo "Find cfstats files and collate"
	echo ""
	echo "Useage: $0 <path> <filename> <keyspace>"
	echo ""
	echo "Note: for filename please put any wilcards inside double quotes"
	exit
fi

# 1. Find all table names in given keyspace
# 2. For each table check each cfstats file for all parameters

# Setup local variables
myPath=$1
myFile=$2
myDir=workingFiles
myKeyspace=$3

# create local workingFiles dir
if [ ! -d ./$myDir ]
then
	mkdir -p ./$myDir
fi

# warn & exit if working directory is not empty
if [ "$(ls -A $myDir)" ]
then
    echo "$myDir is not empty. Exiting"
    exit
fi

# function to find all table names in a given keyspace
function findTables {
for someFile in $(find ./$myPath -name "$myFile")
do
    i=$((i+1))
    tbName=./$myDir/tbs$i
    cfName=./$myDir/cfs$i
    # Note using single quotes prevents variable expansion this had me going for a while!
    # "Table" for later versions of cassandra
    sed -n "/^Keyspace: $myKeyspace/,/^Keyspace/p" $someFile | grep "Table:" > $tbName # Get all tables in the Keyspace
    # "Column Family:" for earlier versions of cassandra
    sed -n "/^Column Family: $myKeyspace/,/^Keyspace/p" $someFile | grep "Column Family:" > $cfName # Get all tables in the Keyspace
    # decide if there were column families or tables found
    if grep -q "Table:" $someFile
    then
       ftb=true
    fi
    # decide if there were column families or tables found
    if grep -q "Column Family:" $someFile
    then
       fcf=true
    fi
done
# warn & exit if both table types were found, there should only be one type
if [ $ftb -a $fcf ]
then
    echo "Found column family and table syntax in files. Exiting"
    exit
fi

# Cat all resulting files and then sort uniquely to get all the tables. The order isnt really important
if [ $ftb ]
then
    for someFile in $(find ./$myDir -name "tbs*")
    do
        cat $someFile >> $myDir/allTables
    done
    cat $myDir/allTables | sort -u > $myDir/uniqueTables
elif [ $fcf ]
then
    for someFile in $(find ./$myDir -name "cfs*")
    do
        cat $someFile >> $myDir/allTables
    done
fi
}

# function to pull out all stats from a given table
function pullStats {
myValue=$1
for someFile in $(find ./$myPath -name "$myFile")
do
    while read someTable 
    do
        echo -e "\n$someTable - $myValue"
	echo -e "======================="
 	grep -A25 -w "$someTable" $someFile | grep -w "$myValue" | awk -F\: '{print $1,$2,$3}'
	grep -A25 -w "$someTable" $someFile | grep -w "$myValue" | awk -F\: '{total += $2} END {print "TOTAL",myValue,total}'
    done < ./$myDir/uniqueTables
done
}

# Find the tables in the given keyspace
findTables 
# Find all the following values, comment out the ones you dont need

if [ $ftb=true ]
then
    # Newer format "Table:"
    pullStats "SSTable count:"
    pullStats "Space used (live), bytes:"
    pullStats "Space used (total), bytes:"
    pullStats "Space used by snapshots (total):"
    pullStats "Off heap memory used (total), bytes:"
    pullStats "SSTable Compression Ratio:"
    pullStats "Number of keys (estimate):"
    pullStats "Memtable cell count:"
    pullStats "Memtable data size, bytes:"
    pullStats "Memtable off heap memory used:"
    pullStats "Memtable switch count:"
    pullStats "Local read count:"
    pullStats "Local read latency:"
    pullStats "Local write count:"
    pullStats "Local write latency:"
    pullStats "Pending tasks:"
    pullStats "Pending flushes:"
    pullStats "Bloom filter false positives:"
    pullStats "Bloom filter false ratio:"
    pullStats "Bloom filter space used, bytes:"
    pullStats "Bloom filter off heap memory used, bytes:"
    pullStats "Index summary off heap memory used, bytes:"
    pullStats "Compression metadata off heap memory used, bytes:"
    pullStats "Compacted partition minimum bytes:"
    pullStats "Compacted partition maximum bytes:"
    pullStats "Compacted partition mean bytes:"
    pullStats "Average live cells per slice (last five minutes):"
    pullStats "Maximum live cells per slice (last five minutes):"
    pullStats "Average tombstones per slice (last five minutes):"
    pullStats "Maximum tombstones per slice (last five minutes):"
elif [ $fcf=true ]
then
    # Older format "Column family:"
    pullStats "Space used (live):"
    pullStats "Space used (total):"
    pullStats "SSTable Compression Ratio:"
    pullStats "Number of Keys (estimate):"
    pullStats "Memtable Columns Count:"
    pullStats "Memtable Data Size:"
    pullStats "Memtable Switch Count:"
    pullStats "Read Count:"
    pullStats "Read Latency:"
    pullStats "Write Count:"
    pullStats "Write Latency:"
    pullStats "Pending Tasks:"
    pullStats "Bloom Filter False Positives:"
    pullStats "Bloom Filter False Ratio:"
    pullStats "Bloom Filter Space Used:"
    pullStats "Compacted row minimum size:"
    pullStats "Compacted row maximum size:"
    pullStats "Compacted row mean size:"
    pullStats "Average live cells per slice (last five minutes):"
    pullStats "Average tombstones per slice (last five minutes):"
fi
exit
