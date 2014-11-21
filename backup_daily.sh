#!/bin/sh

#update spark job daily
scp -p -r /home/erica_li/proj/spark_1D/pandora/modules/luigi_1d/bin  ericali@10.0.2.100:/Users/ericali/working/bkp_201411
scp /etc/spark/conf.dist/spark-env.sh ericali@10.0.2.100:/Users/ericali/working/bkp_201411

#send email
python /home/root/notice.py
