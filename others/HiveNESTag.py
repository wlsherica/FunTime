#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

import luigi

class HiveNESTag(MigoLuigiHive):
    '''
    Job Task:     HiveNESTag
    Objective:    NES tagging for N0, EB, E0, S1, S2, S3
    Author:       Erica L Li
    Created Date: 2015.01.12
    Source:       /user/erica_li/market/adjusted_interval
                  -> shop_id, member_id, grp_mean_adj, 1st_date, last_date
                  /user/erica_li/market/lrfm
                  -> shop_id, member, l, r, f, m
    Destination:  /user/erica_li/market/nes_tag_week
                  -> shop_id, member_id, nes_tag(N7|EB7|E07|S1|S2|S3)
    Usage:        python nes_tag.py HiveNESTag --use-hadoop --src "/user/erica_li/market/adjusted_interval;/user/erica_li/market/lrfm_20140817" --dest /user/erica_li/market/nes_tag --week-month "30"
    Attributes:
    N0: L7D SO,L>0&R<=7, FR=1
    EB: L7D SO,R<=gap_mean_adj&FR>1
    S1: R> 2.0389*gap_mean_adj
    S2: R> 2.5515*gap_mean_adj
    S3: R> 2.9154*gap_mean_adj
    E0: others
    '''
    week_month = luigi.Parameter(default=7)

    def start(self):
        super(HiveNESTag, self).start()

        #week_month = luigi.Parameter(default=7)
        self.srcPool = self.src.split(";")
        self.dest = self.dest + str(self.week_month) 
        self.pathPool = self.srcPool + [self.dest] + [self.week_month] + [self.week_month] + [MIGO_NES_GAMMA_03] + [MIGO_NES_GAMMA_02] + [MIGO_NES_GAMMA_01]

    def requires(self):
        if self.use_hadoop:
            return [CleanerHDFS(filepath) for filepath in self.srcPool]
        else:
            return CleanerLocal(self.srcPool)

    def query(self):
        cmd = """
DROP TABLE adjusted_interval;
CREATE EXTERNAL TABLE adjusted_interval
(
shop_id STRING,
member_id STRING,
grp_mean FLOAT,
first_date STRING,
last_date STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "{}";

DROP TABLE lrfm;
CREATE EXTERNAL TABLE lrfm
(
shop_id STRING,
member_id STRING,
L FLOAT,
R FLOAT,
F FLOAT,
M FLOAT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION "{}";

drop table nes_tag_week;
create external table nes_tag_week
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)
row format delimited fields terminated by '\t'
location "{}";

insert into table nes_tag_week
select x.shop_id,
       x.member_id,
       case when x.L=0 and x.R<={} then 'N'
            when x.R<={} and x.F>1 then 'EB'
            when x.R > y.grp_mean*{} then 'S3'
            when x.R > y.grp_mean*{} then 'S2'
            when x.R > y.grp_mean*{} then 'S1'
            else 'E0' end as NES_Tag
from lrfm as x
join adjusted_interval as y
on x.shop_id = y.shop_id 
and x.member_id = y.member_id
""".format(*self.pathPool)

        self.logger.debug(cmd)
        return cmd

if __name__ == "__main__": 
    luigi.run()
