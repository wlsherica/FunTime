#!/usr/bin/env python

import sys

from athena_variable import *
from athena_luigi import *

import luigi

class ARPU(MigoPeriodHive):
    '''
    Job Task:     arpu
    Objective:    arpu for weekly/monthly
    Author:       Erica L Li
    Created Date: 2015.01.13
    Source:       /user/athena/data_prepare_member
                  -> shop_id, member_id, so_date, amount
                  /user/erica_li/market/nes_tag7
                  -> shop_id, member, nes_tag
    Destination:  /user/erica_li/market/arpu_7
                  -> shop_id, nes_tag(N0 or E0S1S2), arpu
    Usage:        python arpu.py ARPU --use-hadoop --src "/user/athena/data_prepare_member;/user/erica_li/market/nes_tag7" --dest /user/erica_li/market/arpu --week-month "7" --cal-date "20140817" 
    Attributes:
    '''
    week_month = luigi.Parameter(default=7)

    def start(self):

        self.srcPool = self.src.split(";")
        self.dest = self.dest + str(self.week_month)
        self.src = self.srcPool[0]

        self.src_pool = []

        for idx in range(1, self.period+1):
            pasted_date = (datetime.strptime(self.cal_date, "%Y%m%d") - timedelta(idx)).strftime("%Y%m%d")
            folder = "{src}/{date}".format(src=self.src, date=pasted_date)
            if self.hdfsClient.test(folder, "-d"):
                self.src_pool.append(folder)
                self.count_success += int(self.hdfsClient.count(folder).values()[0])
            else:
                # TODO: Miss Target Folder
                self.count_fail += 1
                pass
        self.monitor_request("hivemappersuccess")

        self.locations_for_hive = []
        for location in self.src_pool:
            date = os.path.basename(location)
            self.locations_for_hive.append("ALTER TABLE data_prepare_member ADD PARTITION (date={}) location '{}';".format(date, location))

        self.hive_location = "\n".join(self.locations_for_hive)

    def requires(self):
        if self.use_hadoop:
            return [CleanerHDFS(filepath) for filepath in self.srcPool]
        else:
            return [CleanerLocal(filepath) for filepath in self.srcPool]

    def query(self):
        cmd = """
drop table data_prepare_member;
create external table data_prepare_member
(
shop_id STRING,
member_id STRING,
so_date STRING,
amount float
)
PARTITIONED BY (date STRING)
row format delimited fields terminated by '\t';

{src}

DROP TABLE nes_tag;
CREATE EXTERNAL TABLE nes_tag
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '{srcTag}';

DROP TABLE arpu;
CREATE EXTERNAL TABLE arpu
(
shop_id STRING,
NES_Tagging STRING,
arpu float
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '{dest}';

insert into table arpu
select z.shop_id,
       z.NES_Tagging,
       sum(amount)/count(*) as apru
from 
(
select x.shop_id,
       x.NES_Tag,
       case when NES_Tag = 'N' then 'N0'
       else 'E0S1S2' 
       end as NES_Tagging,
       y.amount
from nes_tag as x
join data_prepare_member as y 
on x.shop_id = y.shop_id and x.member_id = y.member_id
where x.NES_Tag in ('E0','N','S1','S2')
)z
group by z.shop_id, z.NES_Tagging;
""".format(dest=self.dest, srcTag=self.srcPool[1], src=self.hive_location)

        self.logger.debug(cmd)
        return cmd

if __name__ == "__main__": 
    luigi.run()
