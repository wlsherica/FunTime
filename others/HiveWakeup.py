#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from datetime import datetime, timedelta
from athena_variable import *
from athena_luigi import *

import luigi

class Wakeup(MigoPeriodHive):
    '''
    Job Task:     wakeup
    Objective:    wakeup for weekly/monthly
    Author:       Erica L Li
    Created Date: 2015.01.20
    Source:       /user/erica_li/market/nes_tag7/20141201
                  /user/erica_li/market/nes_tag7/20141124
                  -> shop_id, member_id, NES_Tag
    Destination:  /user/erica_li/market/wakeup7
                  -> shop_id, member_id, NES_Tag_current, NES_Tag_ori 
    Usage:        python wakeup.py Wakeup --use-hadoop --src /user/erica_li/market/nes_tag --dest "/user/erica_li/market/wakeup_pre;/user/erica_li/market/wakeup_rate;/user/erica_li/market/tmp" --cal-date "20140817" --period 7

    Attributes:   index1 count(last s1 and this e0)/count(last s1)
                  index2 count(last s2 and this e0)/count(last s2)
                  index3 count(last s3 and this e0)/count(last s3)
                  numerator for member list

    '''
    def start(self):

        self.dest_pool = self.dest.split(";")
        self.dest = self.dest_pool[0] + str(self.period)
        self.dest2 = self.dest_pool[1] + str(self.period)

        self.src_pool = []
        self.target_date = []
        for idx in [0, int(self.period)]:
            pasted_date = (datetime.strptime(self.cal_date, "%Y%m%d") - timedelta(idx)).strftime("%Y%m%d")
            folder = "{src}/{date}".format(src=self.src, date=pasted_date)
            self.target_date.append(pasted_date)
            if self.hdfsClient.test(folder, "-d"):
                self.src_pool.append(folder)
#                self.log("Folder: %s" %folder)
                self.count_success += int(self.hdfsClient.count(folder).values()[0])
            else:
                # TODO: Miss Target Folder
                pass

        self.locations_for_hive = []
        for location in self.src_pool:
            date = os.path.basename(location)
            self.locations_for_hive.append("ALTER TABLE nes_tag ADD PARTITION(date={}) location '{}';".format(date, location))

        self.hive_location = "\n".join(self.locations_for_hive)
        self.monitor_request("hivemappersuccess")

    def query(self):
        cmd = """
drop table nes_tag;
create external table nes_tag
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)
PARTITIONED BY (date STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

{src}

drop table nes_tag_all;
create external table nes_tag_all
(
shop_id STRING,
member_id STRING,
NES_Tag_current STRING,
NES_Tag_ori STRING
)
row format delimited fields terminated by '\t'
location '{dest}';

insert into table nes_tag_all
select x.shop_id,
       x.member_id,
       x.NES_Tag as NES_Tag_current,
       y.NES_Tag as NES_Tag_ori
from nes_tag as x
left join nes_tag as y 
on x.shop_id = y.shop_id and x.member_id = y.member_id
where x.date='{date}' and y.date='{week_ago}'
;

drop table wakeup_rate;
create external table wakeup_rate
(
shop_id STRING,
NES_Tag STRING,
wakeup_rate FLOAT
)
row format delimited fields terminated by '\t'
location '{dest2}';

insert into table wakeup_rate
select x.shop_id, 
       x.NES_Tag_ori as NES_Tag, 
       case when y.cnt is null then 0
       else y.cnt/x.cnt 
       end as wakeup_rate
from
(
select shop_id,
       NES_Tag_ori,
       count(*) as cnt
from nes_tag_all
where NES_Tag_ori in ('S1','S2','S3')
group by shop_id, NES_Tag_ori
) x
left join 
(
select shop_id, 
       wakeg,
       count(*) as cnt
from
(
select shop_id,
       member_id,
       case when NES_Tag_current = 'E0' and NES_Tag_ori = 'S1' then NES_Tag_ori
            when NES_Tag_current = 'E0' and NES_Tag_ori = 'S2' then NES_Tag_ori
            when NES_Tag_current = 'E0' and NES_Tag_ori = 'S3' then NES_Tag_ori
            else null end as wakeg
from nes_tag_all
) x
group by shop_id, wakeg
) y
on x.shop_id = y.shop_id and x.NES_Tag_ori = y.wakeg
;

""".format(dest=self.dest, dest2=self.dest2, src=self.hive_location, date=self.target_date[0], week_ago=self.target_date[1])

        self.log(cmd)
        return cmd

    def output(self):
        return luigi.hdfs.HdfsTarget(self.dest)

class WakeupTA(MigoLuigiHdfs):
    '''
    Job Task:     wakeup member list
    Objective:    wakeup member list for weekly/monthly
    Author:       Erica L Li
    Created Date: 2015.01.20
    Source:       /user/erica_li/market/wakeup
                  -> shop_id, member_id
    Destination:  /user/erica_li/market/tmp
    Usage:       python wake.py WakeupTA --use-hadoop --src /user/erica_li/market/hive/ --dest /user/erica_li/market/tmp
    Attributes:  member list for three index weekly-> 2**16 2**17 2**18; monthly-> 2**19 2**20 2**21
    '''
    period = luigi.Parameter(default=7)

    def start(self):
        self.dest_p = self.dest
        dest3 = self.dest.split(";")[2]
        self.dest = dest3

    def requires(self):
        return Wakeup(use_hadoop=self.use_hadoop, src=self.src, dest=self.dest_p, period=self.period, keep_temp=self.keep_temp, cal_date=self.cal_date)

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
        shop_id, member_id, NES_Tag_current, NES_Tag_ori = infos[0], infos[1], infos[2], infos[3]
        tag = "L"+str(self.period)+"D"

        try:
            if self.period==7 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S1':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_W_S1)
                yield "{}\t{}".format(shop_id, member_id),

            elif self.period==7 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S2':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_W_S2)
                yield "{}\t{}".format(shop_id, member_id),

            elif self.period==7 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S3':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_W_S3)
                yield "{}\t{}".format(shop_id, member_id),

            elif self.period==30 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S1':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_M_S1)
                yield "{}\t{}".format(shop_id, member_id),

            elif self.period==30 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S2':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_M_S2)
                yield "{}\t{}".format(shop_id, member_id),

            elif self.period==30 and NES_Tag_current == 'E0' and NES_Tag_ori == 'S3':
                self.add_ta(shop_id, self.cal_date, tag, member_id, MIGO_TAG1_WAKEUP_M_S3)
                yield "{}\t{}".format(shop_id, member_id),

            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

if __name__ == "__main__":
    luigi.run()
