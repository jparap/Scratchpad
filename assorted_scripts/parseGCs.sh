#!/bin/bash

#
# Script to find GC events
#
# Required: 1 path, to find log files
# Required: 1 prefix for filename
#
# The script will parse a bunch of log files in a
# given directory and pull out the max and min GC
# Cms and ParNew events
#
# Alter the local variables below to configure
# file names.
#
# Note: when using this script don't give a wildcard
# in the filename prefix, so for example if your log
# files are all called "system.log.1", "system.log.2"
# and so on just enter a prefix of "system.log"

if [ $# -ne 2 ];then
	echo "Find GC events and output min / max"
	echo ""
	echo "Useage: $0 <path> <filename prefix>"
	echo ""
	exit 1
fi

# Setup local variables
path=$1
prefix=$2
cmsFile=cmsEvents.txt
parFile=parEvents.txt
tempWorkFile=temp.txt

# function to find cms events and parse them out to a file
function findCms { 
myPath=$1
myPrefix=$2
for someFile in $(find $myPath -name "$myPrefix*")
do
	max=$(grep -H "GC for Concurrent" $someFile | \
	awk ' \
		{if (gcMax[$1] < $12) \
			{gcMax[$1]=$12; \
			} \
		} \
		END \
		{for(i in gcMax) 
			{print gcMax[i]}
		}')

	min=$(grep -H "GC for Concurrent" $someFile | \
	awk ' \
		{if (gcMin[$1] > $12 || gcMin[$1]==0) \
			{gcMin[$1]=$12; \
			} \
		} \
		END \
		{for(i in gcMin) \
			{print gcMin[i]} \
		}')

	if [ -z $min ];then
		echo -e "$someFile,no events" >> $tempWorkFile
	else
		echo -e "$someFile,$max,$min" >> $tempWorkFile
	fi
done
echo -e "filename,max (ms),min (ms)" > $cmsFile
echo -e "" >> $cmsFile
cat $tempWorkFile | sort -t, -rg -k2 >> $cmsFile
cat /dev/null > $tempWorkFile
}
 
# function to find parnew events and parse them out to a file
function findParNew { 
myPath=$1
myPrefix=$2
for someFile in $(find $myPath -name "$myPrefix*")
do
	max=$(grep -H "GC for ParNew" $someFile | \
	awk ' \
		{if (gcMax[$1] < $12) \
			{gcMax[$1]=$12; \
			} \
		} \
		END \
		{for(i in gcMax) \
			{print gcMax[i]}
		}')

	min=$(grep -H "GC for ParNew" $someFile | \
	awk ' \
		{if (gcMin[$1] > $12 || gcMin[$1]==0) \
			{gcMin[$1]=$12; \
			} \
		} \
		END \
		{for(i in gcMin) \
			{print gcMin[i]} \
		}')

	if [ -z $min ];then
		echo -e "$someFile,no events" >> $tempWorkFile
	else
		echo -e "$someFile,$max,$min" >> $tempWorkFile
	fi
done
echo -e "filename,max (ms),min (ms)" > $parFile
echo -e "" >> $parFile
cat $tempWorkFile | sort -t, -rg -k2 >> $parFile
cat /dev/null > $tempWorkFile
}

# find the cms events
findCms $path $prefix
# find the parnew events
findParNew $path $prefix
# clean up
rm $tempWorkFile
