#!/bin/bash
# A shell script you can give to customers to check if all their repair sessions are complete.
# The following arguments are optional, the default is cwd and system.log* 
# The first argument is the path to the files
# The second argument is the file pattern

SYSTEM_LOG_PATH=.
LOG_FILE_PATTERN="system.log*"

if [ $# -eq 1 ];then
 if [ "$1" = "-h" ] || [ "$1" = "-?" ] ; then
 echo "Usage: $0 [optional-path] [optional log pattern]" 
 echo 'By default the path is . and the pattern is system.log*'
 exit
 fi
SYSTEM_LOG_PATH=$1
fi

if [ $# -eq 2 ];then
SYSTEM_LOG_PATH=$1
LOG_FILE_PATTERN=$2
fi

cd $SYSTEM_LOG_PATH 2>/dev/null 
if [ $? -ne 0 ] ;  then
 echo ""
 echo "Could not change to directory $SYSTEM_LOG_PATH"
 exit 1
fi

for FILE in `ls -C1 $LOG_FILE_PATTERN 2>/dev/null`
do
 FILES="$FILES $FILE"
done

if [ "x$FILES" = "x" ] ; then
 echo ""
 echo "No files matching pattern $LOG_FILE_PATTERN found"
 exit 1
fi

NEW_SES="/tmp/new-session.$$"
COMP_SUC="/tmp/completed-successfully.$$" 
#grep 'new session' 
egrep '\[repair \S+\] new session' $FILES | cut -d# -f2 | cut -d] -f1 | sort | uniq > $NEW_SES
#grep 'completed successfully'
egrep '\[repair \S+\] session completed successfully' $FILES | cut -d# -f2 | cut -d] -f1 | sort | uniq > $COMP_SUC
INCOMPLETE_SESSIONS=$(grep -vf  $COMP_SUC $NEW_SES)
COMPLETED=`cat $COMP_SUC| wc -l`
INITIATED=`cat $NEW_SES| wc -l`
#rm $COMP_SUC $NEW_SES
cd - >/dev/null
if [ -z "$INCOMPLETE_SESSIONS" ]; then
    echo All sessions complete, total: $COMPLETED
    exit 0
else
    echo Initiated: $INITIATED Completed: $COMPLETED
    echo The following sessions are not complete:
    echo $INCOMPLETE_SESSIONS
    exit 255
fi
