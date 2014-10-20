#!/bin/bash
#
#Looks into the repair sessions started and looks how long it takes them to completion
#
# Required: 1 path or file

if [ $# -ne 1 ];then
	echo "Repair session check"
	echo ""
	echo "Useage: $0 <path1>"
	echo ""
	exit 1
fi
path1=$1
for currentFile in $path1*
do
	listFiles=$listFiles" "$currentFile
done
for someFile in $path1*
do
	echo "checking: $someFile"
	for i in $(egrep RepairSession.java $someFile $listFiles | egrep -i  new | sed -e 's/^.*\[repair \(#.*\)\] new .*$/\1/g'); do
    		i=${i/\#/}
		echo "	repair range $i"
		start=$(awk '/'${i}'/ && /new session/{print $3,$4,$NF}' $listFiles)
		table=${start##* }
		echo "	table: $table"
		start=${start% *}
		echo "	start: $start"
		ended=$(awk '/'${i}'/ && /completed/{print $3,$4}' $listFiles)
		echo "	ended: $ended"
		start1=$(echo ${start} | sed -e 's/[-:]/ /g' -e "s/,/./g" | awk '{print $1"-"$2,$3*86400+$4*3600+$5*60+$6}')
		echo "	start1: $start1"
		ended1=$(echo ${ended} | sed -e 's/[-:]/ /g' -e "s/,/./g" | awk '{print $1"-"$2,$3*86400+$4*3600+$5*60+$6}')
		echo "	ended1: $ended1"
		timed=$(echo "${start1} ${ended1}" | awk '{print $4-$2}')
		echo "	timed: $timed"
		if [ ${timed} -gt 3600 ]; then
			echo "Repair Session ${i} for ${table}"
			echo "Start  : ${start}"
			echo "End    : ${ended}"
			echo "Elapsed: ${timed} secs"
			echo "============="
    		fi 
	done
done
