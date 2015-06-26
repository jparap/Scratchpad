#!/bin/bash
#
# Grab spark UI from a given IP
#
# 1. Get the first page from the master
masterIp=$1
wget "$masterIp:7080" -O master.html

# 2. Get the worker IPs
workerIps=$(grep "worker-" master.html | awk -F "//" '{print $2}' | awk -F ":" '{printf $1","}')
echo "Found the following worker IPs: $workerIps"

# 3. Get all the pages recursively
wget -k -m --adjust-extension --span-hosts "$masterIp:7080" -D ''"$workerIps"''
