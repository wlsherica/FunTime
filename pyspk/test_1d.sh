#!/bin/sh

rm -rf /home/training/erica/pyspk/result*
rm -rf /home/training/erica/pyspk/F1D.json

cd /data/spark/spark-1.0.2-bin-hadoop2

./bin/spark-submit /home/training/erica/pyspk/test_1d.py

cd /home/training/erica/pyspk
