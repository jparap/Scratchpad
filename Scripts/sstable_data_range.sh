#!/bin/bash
#
# Find the date range of some given sstables
#
# Required:
# 1 - path to sstables
#
#

if [ $# -ne 1 ];then
        echo "fimd date range of sstables"
        echo ""
        echo "Useage: $0 <path>"
        echo ""
        exit 1
fi

path=$1
myfile="*Data*"

for somefile in $(find $path -maxdepth 1 -name "*Data*")
do
    mints=$(sstablemetadata $somefile | grep "Minimum timestamp:")
    maxts=$(sstablemetadata $somefile | grep "Maximum timestamp:")
    mintsnorm=$(echo $mints | awk '{print substr($3,0,length($3)-5)}')
    maxtsnorm=$(echo $maxts | awk '{print substr($3,0,length($3)-5)}')
    mindate=$(date -d @$mintsnorm)
    maxdate=$(date -d @$maxtsnorm)
    echo -e "$somefile\t$mindate\t$maxdate"
done
