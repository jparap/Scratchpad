#!/bin/bash
#
# Grab spark UI from a given IP
#
if [ $# -ne 1 ];then
        echo "Copy the spark UI to local files for browsing"
        echo ""
        echo "Useage: $0 <master IP address>"
        echo ""
        exit 1
fi

# 1. Get the first page from the master
masterIp=$1
wget "$masterIp:7080" -O master.html

# 2. Get the worker IPs
workerIps=$(grep "worker-" master.html | awk -F "//" '{print $2}' | awk -F ":" '{printf $1","}')
echo "Found the following worker IPs: $workerIps"

# 3. Get all the pages recursively
wget -q -k -m --adjust-extension --span-hosts "$masterIp:7080" -D ''"$workerIps"''
