#!/bin/sh

hdfsPath=/user
usage="Usage: dir_diff.sh [days] [hdfsfolder]"
now=$(date +%s)

days=$1
days=7
hdfsfolder=$2
hdfsfolder='tmp athena pandora'

for target in $(echo ${hdfsfolder});
do
    hadoop fs -test -e ${hdfsPath}/${target}
    ret=$?
    if [ ${ret} -eq 0 ]; then
        hadoop fs -ls ${hdfsPath}/${target} | grep 'user' | awk '{print $8}' | while read f; do

            dir_date=$(hadoop fs -stat $f | awk '{print $1}')
            difference=$((($now - $(date -d "$dir_date" +%s))/(24*60*60)))

            if [ $difference -gt $1 ]; then
                hadoop fs -rm -r ${f}
                echo "Successfully remove ${f} from hdfs"
            fi
        done
    else
        echo "File does not exist ${hdfsPath}/${target}"
    fi
done
