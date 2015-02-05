import sys

from datetime import datetime, timedelta
from athena_variable import *
from athena_luigi import *

import luigi
import urllib, urllib2

import commands

try:
    import jsonlib2 as json
except:
    import json

class StoreReport(MigoPeriodHive):
    '''
    Job Task:     store report
    Objective:    store report(weekly/monthly) and store API for mongo
    Author:       Erica L Li
    Created Date: 2015.01.26
    Source:       
                  /user/athena/validmemberrate/20140817
                      shop_id, TA_Tag(2**12,2**13,2**14), validmemberrate
                      kgsupermarket^C006      16384   -0.0215000495393
                  /user/erica_li/market/conversion -> /user/athena/conversion_week/20140817
                      shop_id, conversion_rate
                      kgsupermarket   0.99301416
                  /user/erica_li/market/wakeup -> /user/athena/wakeup_week/20140817
                      shop_id, TA_Tag(S1,S2,S3), wakeup_rate
                      kgsupermarket   S1      0.0
                  /user/athena/activeness
                      kgsupermarket^C002      0.000132036050871
                  /user/erica_li/market/arpu -> /user/athena/arpu_week/20140817
                      shop_id, nes_tag(N0(N+EB) or E0S1S2), arpu
                      kgsupermarket   N0      87.35619
                  /user/erica_li/market/nes_tag -> /user/athena/nes_tag_week
                      shop_id, member_id, NES_Tag

                  /user/erica_li/market/shopall
                      shop_id 

    Destination:  /user/erica_li/market/report
                  pandora API
                   
    Usage:        python store_report.py StoreReport --use-hadoop --src "/user/erica_li/market/shopall;/user/athena/nes_tag_week;/user/athena/arpu_week;/user/athena/activeness;/user/athena/validmemberrate;/user/athena/conversion_week;/user/athena/wakeup_week" --dest "/user/erica_li/market/report" --cal-date "20140817" --period 7 
    Attributes:   ARPU[*,*,-999,-999,-999]
                  Active[-999,*,-999,-999,-999]
                  AddedRate[*,-999,*,-999,-999]
                  ChurnRate[-999,-999,-999,-999,*]
                  ConversionRate[*,-999,-999,-999,-999]
                  ValidChangeNumber[*(N0+EB),*,*,*,*]
                  WakeUpRate[-999,-999,*,*,*]
    '''
    def start(self):

        self.src_pool = self.src.split(";")
        self.shops = self.src_pool[0]

        self.srcPool = []
        self.target = []
        for idx in range(1, len(self.src_pool)):
            folder = "{}/{}".format(self.src_pool[idx], str(self.cal_date))
            if self.hdfsClient.test(folder, "-d"):
                self.srcPool.append(folder)
                self.count_success += int(self.hdfsClient.count(folder).values()[0])
            else:
                # TODO: Miss Target Folder
                pass

        self.srcPool.insert(0, "".join(self.shops))
        self.target = list(self.srcPool)
        self.target.append(self.dest)

        self.monitor_request("hivemappersuccess")

    def requires(self):
        if self.use_hadoop:
            return [CleanerHDFS(filepath) for filepath in self.srcPool]
        else:
            return CleanerLocal(self.srcPool)

    def query(self):
        cmd = """
drop table main_store;
create external table main_store
(
shop_id STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '{}';

DROP TABLE nes_tag7;
CREATE EXTERNAL TABLE nes_tag7
(
shop_id STRING,
member_id STRING,
NES_Tag STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '{}';

drop table arpu;
create external table arpu
(
shop_id STRING,
NES_Tag STRING,
arpu FLOAT
)
row format delimited fields terminated by '\t'
location '{}';

drop table active;
create external table active
(
shop_id STRING,
active FLOAT
)
row format delimited fields terminated by '\t'
location '{}';

drop table valid_member;  
create external table valid_member
(
shop_id STRING,
TA_Tag STRING,
rate FLOAT
)
row format delimited fields terminated by '\t'
location '{}';

drop table conversion;
create external table conversion
(
shop_id STRING,
conversion_rate FLOAT
)
row format delimited fields terminated by '\t'
location '{}';

drop table wakeup;
create external table wakeup
(
shop_id STRING,
TA_Tag STRING,
wakeup_rate FLOAT
)
row format delimited fields terminated by '\t'
location '{}';

drop table report;
create external table report
(
shop_id STRING,
category STRING,
N STRING,
E STRING,
S1 STRING,
S2 STRING,
S3 STRING
)
row format delimited fields terminated by '\t'
location '{}';









insert into table report
select a.shop_id,
       'wakeuprate' as category,
       -999 as N,
       -999 as E,
       case when b.wakeup_rate is null then -999 else b.wakeup_rate end as S1,
       case when c.wakeup_rate is null then -999 else c.wakeup_rate end as S2,
       case when d.wakeup_rate is null then -999 else d.wakeup_rate end as S3
from main_store as a
left join
(
select shop_id, 
       wakeup_rate
from wakeup
where ta_tag = 'S1'
)b
on a.shop_id = b.shop_id
left join
(
select shop_id, 
       wakeup_rate
from wakeup
where ta_tag = 'S2'
)c
on a.shop_id = c.shop_id
left join
(
select shop_id, 
       wakeup_rate
from wakeup
where ta_tag = 'S3'
)d
on a.shop_id = d.shop_id;

insert into table report
select a.shop_id,
       'validchangenumber' as category,
       case when e.cnt is null then -999 else e.cnt end as N,
       case when f.cnt is null then -999 else f.cnt end as E,
       case when b.cnt is null then -999 else b.cnt end as S1,
       case when c.cnt is null then -999 else c.cnt end as S2,
       case when d.cnt is null then -999 else d.cnt end as S3
from main_store as a
left join
(
select shop_id,
       count(*) as cnt
from nes_tag7
where NES_Tag in ('EB','N')
group by shop_id
)e
on a.shop_id = e.shop_id
left join
(
select shop_id,
       count(*) as cnt
from nes_tag7
where NES_Tag ='E0'
group by shop_id
)f
on a.shop_id = f.shop_id
left join
(
select shop_id,
       count(*) as cnt
from nes_tag7
where NES_Tag ='S1'
group by shop_id
)b
on a.shop_id = b.shop_id
left join
(
select shop_id,
       count(*) as cnt
from nes_tag7
where NES_Tag ='S2'
group by shop_id
)c
on a.shop_id = c.shop_id
left join
(
select shop_id,
       count(*) as cnt
from nes_tag7
where NES_Tag ='S3'
group by shop_id
)d
on a.shop_id = d.shop_id
;


insert into table report
select a.shop_id,
       'activeness' as category, 
       -999 as N,
       case when w.active is null then -999
       else w.active end as E,
       -999 as S1,
       -999 as S2,
       -999 as S3
from main_store as a
left join active as w
on a.shop_id = w.shop_id;

insert into table report
select a.shop_id,
       'conversion' as category,
       case when r.conversion_rate is null then -999
       else r.conversion_rate end as N, 
       -999 as E,
       -999 as S1,
       -999 as S2,
       -999 as S3
from main_store as a
left join conversion as r
on a.shop_id = r.shop_id;

insert into table report
select a.shop_id,
       'arpu' as category,
        case when p.arpu is null then -999 else p.arpu end as N,
        case when q.arpu is null then -999 else q.arpu end as E,
       -999 as S1,
       -999 as S2,
       -999 as S3
from main_store as a
left join
(
select shop_id,
       arpu
from apru
where NES_Tag = 'N0'
)p
on a.shop_id = p.shop_id
left join
(
select shop_id,
       arpu
from apru
where NES_Tag = 'E0S1S2'
)q
on a.shop_id = q.shop_id;

insert into table report
select o.shop_id,
       'addedrate' as category,
       m.N,
       -999 as E,
       m.S1,
       -999 as S2,
       -999 as S3 
from main_store as o
left join 
(
select shop_id,
       collect_list(rate)[0] as N,
       collect_list(rate)[1] as S1
from valid_member 
where ta_tag in (4096, 8192)
group by shop_id
)m 
on o.shop_id = m.shop_id
;

insert into table report
select v.shop_id,
       'churnrate' as category,
       -999 as N,
       -999 as E,
       -999 as S1,
       -999 as S2,
       s.rate as S3
from main_store as v
left join 
(
select shop_id,
       rate
from valid_member 
where ta_tag =16384
)s
on v.shop_id = s.shop_id
;

""".format(*self.target)

        self.log(cmd)
        return cmd

    def end(self):
        periodtype = "L" + str(self.period) + "D"
        caldate = datetime.strptime(self.cal_date, "%Y%m%d").strftime("%Y-%m-%d 00:00:00")

        report = {"ConversionRate":"","AddedRate":"","ValidChangeNumber":"","ChurnRate":"","ARPU":"","Active":"","WakeUpRate":""}
        report2 = {"StoreID":"", "PeriodRecords":"","PeriodType": periodtype, "LastEditDate": caldate, "ShopID": ""}

        kpimap = {"wakeuprate":"WakeUpRate","conversion":"ConversionRate","addedrate":"AddedRate","validchangenumber":"ValidChangeNumber","churnrate":"ChurnRate","arpu":"ARPU","activeness":"Active"}

        cmd = "hadoop fs -text {}/* | sort -k1,1 ".format(self.dest)
        status, output = commands.getstatusoutput(cmd)
        line = output.split("\n")

        data = []        
        shop_id = None

        for i in line:
            infos = i.split("\t")
            shop = infos[0]
            category = infos[1]
            kpi = infos[2:]

            if shop_id != shop and shop_id != None:
                report2["StoreID"] = shop_id
                report2["PeriodRecords"] = report
                data.append(report2)
                report = {"ConversionRate":"","AddedRate":"","ValidChangeNumber":"","ChurnRate":"","ARPU":"","Active":"","WakeUpRate":""}
                report2 = {"StoreID":"", "PeriodRecords":"","PeriodType": periodtype, "LastEditDate": caldate, "ShopID": ""}
            if category in kpimap.keys():
                report[kpimap[category]] = kpi
            shop_id = shop

        report2["StoreID"] = shop_id
        report2["PeriodRecords"] = report
        data.append(report2)

        shop_name = data[0]['StoreID'].split("^")[0]

        for info in data:
            info["ShopID"] = shop_name
            myval = json.dumps(info)
            postval = urllib.quote(myval)
            url = "http://pandora-dev.migocorp.com:8000/lemon/starterdiy/store/i/{val}/".format(val=postval)
            req = urllib2.Request(url)

            try:
                urllib2.urlopen(req)
            except Exception as e:
                import logging.handlers

                my_logger = logging.getLogger('AthenaLogger')

                handler = logging.handlers.SysLogHandler(facility=logging.handlers.SysLogHandler.LOG_LOCAL6, address="/dev/log")

                formatter = logging.Formatter('[%(name)s] [%(levelname)s] - %(message)s')
                handler.setFormatter(formatter)

                my_logger.addHandler(handler)
                my_logger.warn("Failure{err} in sending monitor request - {url}".format(err=str(e), url=url))

            self.count_success = 0
            self.count_fail = 0

if __name__ == "__main__":
    luigi.run()
