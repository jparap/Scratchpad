#!/bin/bash
#
#Looks into the repair sessions started and looks how long it takes them to completion
#
# Required: 1 path or file

# Check arguments length
if [ $# -ne 2 ];then
	echo "Repair session check"
	echo ""
	echo "Useage: $0 <path1> <min threshold seconds>"
	echo ""
	echo "Checks a given set of logs for repair sessions"
	echo "Takes a path (or file) and an amount of seconds"
	echo "as a minumum threshold"
	echo ""
	echo "So if you wanted all repairs > 3600 seconds"
	echo ""
	echo "$0 /home/user/logs/ 3600"
	exit 1
fi

# Setup local variables
path1=$1
minsecs=$2
fileCount=0
fileNum=0
passCount=0

# Find all files in path
for currentFile in $path1*
do
	listFiles=$listFiles" "$currentFile
	let "fileCount += 1"
done
echo "found $fileCount files"

# Main loop, checks through each file for the evidence of a repair session starting
# then pulls out the table name, start and end date to get a total elapsed time to report
for someFile in $path1*
do
	echo "checking: $someFile"
	echo "============="
	let "fileNum += 1"
	for i in $(egrep RepairSession.java $listFiles | egrep -i  new | sed -e 's/^.*\[repair \(#.*\)\] new .*$/\1/g'); do
		let "passCount += 1"
		echo -ne "Checking $someFile: $fileNum of $fileCount files, pass $passCount, repair range $i\r"
    		i=${i/\#/}
		start=$(awk '/'${i}'/ && /new session/{print $3,$4,$NF}' $listFiles)
		table=${start##* }
		start=${start% *}
		ended=$(awk '/'${i}'/ && /completed/{print $3,$4}' $listFiles)
		start1=$(echo ${start} | sed -e 's/[-:]/ /g' -e "s/,/./g" | awk '{print $1"-"$2,$3*86400+$4*3600+$5*60+$6}')
		ended1=$(echo ${ended} | sed -e 's/[-:]/ /g' -e "s/,/./g" | awk '{print $1"-"$2,$3*86400+$4*3600+$5*60+$6}')
		timed=$(echo "${start1} ${ended1}" | awk '{print $4-$2}')
		if [ ${timed} -gt ${minsecs} ]; then
			echo "Repair Session ${i} for ${table}"
			echo "Start  : ${start}"
			echo "End    : ${ended}"
			echo "Elapsed: ${timed} secs"
			echo "============="
    		fi 
	done
done
