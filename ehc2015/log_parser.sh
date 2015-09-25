#!/bin/bash
#Objective: parser log file
#Usage: sh -x log_parser.sh

actionCount=0

while read line; do
    actionBool=0
    if [[ "$line" = *group=ec* ]]; then
        actionBool=1
        (( actionCount++ ))
    fi
done < /Users/etu/data/log.txt
echo "Ans. There are $actionCount action keywords from log."


