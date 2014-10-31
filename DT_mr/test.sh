#!/bin/sh

hadoop fs -rm -r /user/training/el16_test/output/4test

j=$(hadoop fs -ls /user/training/el16_test/mr/Sunny/mr002 | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep outcome | cut -f2; done)

hadoop jar /opt/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.47/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input "/user/training/el16_test/mr/Sunny/mr00*" \
-output "/user/training/el16_test/output/4test" \
-file "/home/training/erica/DT_mr/mapper_ent_igr.py" \
-file "/home/training/erica/DT_mr/reducer_ent_igr.py" \
-mapper "mapper_ent_igr.py ${j}" \
-reducer "reducer_ent_igr.py"
#-D mapred.reduce.tasks=1 \

hadoop fs -getmerge /user/training/el16_test/output/4test text.txt

