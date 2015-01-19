#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

import luigi

class Conversion(MigoPeriodHive):
    '''
    Job Task:     conversion
    Objective:    conversion for weekly/monthly
    Author:       Erica L Li
    Created Date: 2015.01.15
    Source:       /user/erica_li/market/nes_tag7
                  -> shop_id, member_id, NES_Tag
    Destination:  /user/erica_li/market/conversion
                  -> shop_id, conversion
    Usage:        python conversion.py CONVERSION --use-hadoop -src /user/erica_li/market/nes_tag --dest user/erica_li/market/conversion --period 7
    Attributes:  conversion = count(EB)/count(N+EB)
    '''
    def start(self):

        self.dest_p = self.dest.split(";")
        self.dest2 = self.dest_p[1]
        self.dest1 = self.dest_p[0]

    def query(self):
        cmd = """
drop table nes_tag;
create external table nes_tag
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)   
row format delimited fields terminated by '\t'
location '{src}';

DROP TABLE conversion;
CREATE EXTERNAL TABLE conversion
(
shop_id STRING,
conversion float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '{dest1}';

insert into table conversion
select y.shop_id,
       case when y.EB is not null then y.EB/(y.N+y.EB) 
       else -999
       end as conversion
from 
(
select shop_id,
       collect_list(cnt)[1] as EB,
       collect_list(cnt)[2] as N
from
(
select shop_id, 
       NES_Tag,
       count(*) as cnt
from nes_tag
group by shop_id, NES_Tag
order by shop_id, NES_Tag
)x
group by shop_id
)y      
;
 
drop table conversion_eb_list;
create external table conversion_eb_list
(
shop_id STRING,
member_id STRING
)
row format delimited fields terminated by '\t'
location '{dest2}';

insert into table conversion_eb_list
select shop_id,
       member_id
from nes_tag
where NES_Tag = 'EB'
;
""".format(dest1=self.dest1, src=self.src, dest2=self.dest2)

        self.logger.debug(cmd)
        return cmd

    def output(self):
        return luigi.hdfs.HdfsTarget(self.dest2)

    def on_success(self):
        pass

class ConversionTA(MigoLuigiHdfs):
    '''
    Job Task:     conversion member list
    Objective:    conversion member list for weekly/monthly
    Author:       Erica L Li
    Created Date: 2015.01.19
    Source:       /user/erica_li/market/conversion_eb_list
                  -> shop_id, member_id
    Destination:  /user/erica_li/market/tmp
    Usage:       python testTA.py Conversion_TA --use-hadoop --src /user/erica_li/market/hive/conversion_eb_list --dest /user/erica_li/market/tmp
    Attributes:  member list for EB, 2**18, 2**19
    '''
    period = luigi.Parameter(default=7)

    def start(self):
#src "/user/erica_li/market/nes_tag" 
#dest "/user/erica_li/market/conversion;/user/erica_li/market/hive/conversion_eb_list;/user/erica_li/market/tmp"
        dest_p = self.dest.split(";")
        self.src = self.src + str(self.period)

        self.dest3 = dest_p[2]
        self.dest1 = dest_p[0] + str(self.period) + ';' + dest_p[1]
        self.dest = self.dest3

    def requires(self):
        return Conversion(use_hadoop=self.use_hadoop, src=self.src, dest=self.dest1, period=self.period, keep_temp=self.keep_temp)

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
        shop_id, member_id = infos[0], infos[1]

        try:
            self.count_success += 1
            #self.add_ta(shop_id, self.cal_date, "L7D", member_id, eval("2**18"))
            yield "{}\t{}".format(shop_id, member_id), 
        except Exception as e:
            self.count_fail += 1

if __name__ == "__main__": 
    luigi.run()
