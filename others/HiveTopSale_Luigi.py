#!/usr/bin/env python

#Objective: Ranked item amount, qt separately in hive
#Author: Erica L Li
#Created Date: 2014.12.30
#: python ~/src/luigi/examples/do_hive.py MyHiveQueryTask --local-scheduler

import sys

from athena_variable import *
from athena_luigi import *

import luigi, luigi.hadoop, luigi.hdfs, luigi.hive

class MyHiveQueryTask(luigi.hive.HiveQueryTask, MigoLuigiHdfs):

    def remove_files(self):
        if not self.keep_temp:
            return [self.dest]
        else:
            return []

    def requires(self):
        if self.use_hadoop:
            return CleanerHDFS(self.src)
        else:
            return CleanerLocal(self.src)

    def output(self):
        if self.use_hadoop:
            return luigi.hdfs.HdfsTarget(self.dest, format=luigi.hdfs.PlainDir)
        else:
            return luigi.LocalTarget(self.dest)

    def query(self):
        return """
DROP TABLE itemrevenue;
CREATE EXTERNAL TABLE itemrevenue 
(
shop_id STRING,
item_id STRING,
item_qt INT,
item_amt FLOAT
)
row format delimited fields terminated by '\t'
location '/user/erica_li/market/itemrevenue';
 
DROP TABLE itemtopsale_hive;
CREATE EXTERNAL TABLE itemtopsale_hive
(
shop_id STRING,
item_id STRING,
item_qt INT,
rank_qt INT,
item_amt FLOAT,
rank_amt INT
)
row format delimited fields terminated by '\t'
location '{dest}';

insert into table itemtopsale_hive
select shop_id,
       item_id,
       item_qt,
       rank() over (partition by shop_id order by item_qt desc) as rank_qt,
       item_amt,
       rank() over (partition by shop_id order by item_amt desc) as rank_amt
from   itemrevenue
;
""".format(dest=self.dest)

    def _end(self, code):
        client = luigi.hdfs.HdfsClient()
        result = client.count(self.dest)
        count_size = result.values()[0]
        self.count_success = int(result.values()[0])
        self.monitor_request("hivesuccess")

        self.end(code)

if __name__ == '__main__':
    luigi.run()
