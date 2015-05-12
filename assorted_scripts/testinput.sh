#!/bin/bash

# Testing input for wildcard
set -f
myfiles1=$(find . -name $1)
myfiles2=`find . -name $1`
set +f
echo "one $myfiles1"
echo "two $myfiles2"
