#!/bin/sh

# get hdfs file creation time

hdfsPath=/user
usage="Usage: dir_diff.sh [days]"
now=$(date +%s)

if [ ! "$1" ]
then
    echo $usage
    exit 1
fi

for target in $(echo tmp);
do
    echo "${hdfsPath}/${target}"
    hadoop fs -ls ${hdfsPath}/${target} | grep 'user' | awk '{print $8}' | while read f; do

        dir_date=$(hadoop fs -stat $f | awk '{print $1}')
        difference=$((($now - $(date -d "$dir_date" +%s))/(24*60*60)))

        echo "Now datetime: ${now}, and this is dir_date: ${dir_date}, diffdate: ${difference}"

        if [ $difference -gt $1 ]; then
            echo "hadoop fs -rm -r ${f}"
            echo "Successfully remove ${f} from hdfs"
        fi
    done
#    ret=$?
#    if [ ${ret} -eq 0 ]; then
#        echo "Successfully remove ${hdfsPath}/${target} from hdfs"
#    else
#        echo "Terminated because the remove  failure of ${hdfsPath}/${target}"
#        exit 4
#    fi
done
