#!/bin/bash

#
# Script to find GC events
#
# Required: 1 path, to find log files
# Required: 1 prefix for filename

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
	max=$(grep -H "GC for Concurrent" $someFile | awk '{if (myArray[$1] < $12) myArray[$1]=$12;}END{for(i in myArray){print i,myArray[i]"ms";}}' OFS=\\\t)
	min=$(grep -H "GC for Concurrent" $someFile | awk '{myArray[$1]=999999999; if (myArray[$1] > $12) myArray[$1]=$12;}END{for(i in myArray){print myArray[i]"ms";}}')
	if [ -z $min ];then
		echo -e "$someFile\tno events" >> $tempWorkFile
	else
		echo -e "$max\t$min" >> $tempWorkFile
	fi
done
echo -e "filename\tmax\tmin" > $cmsFile
echo -e "" >> $cmsFile
cat $tempWorkFile | sort -k1 >> $cmsFile
cat /dev/null > $tempWorkFile
}
 
# function to find parnew events and parse them out to a file
function findParNew { 
myPath=$1
myPrefix=$2
for someFile in $(find $myPath -name "$myPrefix*")
do
	max=$(grep -H "GC for ParNew" $someFile | awk '{if (myArray[$1] < $12) myArray[$1]=$12;}END{for(i in myArray){print i,myArray[i]"ms";}}' OFS=\\\t)
	min=$(grep -H "GC for ParNew" $someFile | awk '{myArray[$1]=999999999; if (myArray[$1] > $12) myArray[$1]=$12;}END{for(i in myArray){print myArray[i]"ms";}}')
	if [ -z $min ];then
		echo -e "$someFile\tno events" >> $tempWorkFile
	else
		echo -e "$max\t$min" >> $tempWorkFile
	fi
done
echo -e "filename\tmax\tmin" > $parFile
echo -e "" >> $parFile
cat $tempWorkFile | sort -k1 >> $parFile
cat /dev/null > $tempWorkFile
}

# find the cms events
findCms $path $prefix
# find the parnew events
findParNew $path $prefix
# clean up
rm $tempWorkFile
