#!/bin/sh

file2="/home/training/erica/DT_mr/mr/desc_0_root.txt"
file1="/home/training/erica/DT_mr/mr/node_0_root.txt"
file3="/home/training/erica/DT_mr/mr/root_json"
level=0
pythonUtil=exportJson.py

#pythonUtil=screwDriver.py

python ${pythonUtil} -p ${level} -n ${file1} -d ${file2} -e ${file3}

#j=$(hadoop fs -ls /user/training/el16_test/mr/Sunny/mr002 | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep outcome | cut -f2; done)

#cat /home/training/erica/DT_mr/text.txt | python mapper_ent_igr.py ${j} | sort | python reducer_ent_igr.py

