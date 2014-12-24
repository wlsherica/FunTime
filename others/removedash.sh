#!/bin/sh
#Remove dash from hdfs file name

hdfs=/user/data

#hadoop fs -ls ${hdfs} | awk '{print $8}'
#ls | awk '{print $1}'
for files in $(hadoop fs -ls ${hdfs} | awk '{print $8}')
do
    ori=${files}
    new=${files//-/}
    hadoop fs -mv ${ori} ${new}
done
