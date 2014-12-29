#!/bin/sh

hdfsPath=/user
usage="Usage: dir_diff.sh [days]"
now=$(date +%s)

if [ ! "$1" ]
then
    echo $usage
    exit 1
fi

for target in $(echo tmp2222 athena pandora);
do
    echo "${hdfsPath}/${target}"
    hadoop fs -test -e ${hdfsPath}/${target}
    ret=$?
    if [ ${ret} -eq 0 ]; then
        echo "File exists"
    else
        echo "File does not exist ${hdfsPath}/${target}"
        #exit 4
    fi
done
