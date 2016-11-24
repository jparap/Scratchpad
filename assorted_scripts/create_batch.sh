#!/bin/bash
LOOP=$(($1))
COL1=$1
COL2=$2
COL3=$3
echo "BEGIN BATCH" > batch_cmds_$COL1

for ((idx; idx < $1; idx++))
do
    echo "INSERT INTO myks.tablea (col1, col2, col3) values ('$idx', '$COL2', '$COL3');" >> batch_cmds_$COL1
    echo "INSERT INTO myks.tableb (col1, col2, col3) values ('$idx', '$COL2', '$COL3');" >> batch_cmds_$COL1
done
echo "APPLY BATCH;" >> batch_cmds_$COL1
