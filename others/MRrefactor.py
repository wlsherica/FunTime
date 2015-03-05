#!/usr/bin/python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *
from athena_util import *
from athena_math import *

import math
import numpy as np

from datetime import datetime
from datetime import date, timedelta as td

import luigi, luigi.hadoop, luigi.hdfs

class Mu2Interval(MigoLuigiHdfs):
    '''
    Objective:    For each shop_id, extract all the perchasing intervals of its members and make some statistical calculations.
    Job Task:     Mu2Interval
    Author:       Erica L Li
    Created Date: 2015.02.13
    Source:       /user/erica_li/market/trans20.dat
                  <shop_id> <member_id> <order date> <money>
    Destination:  /user/erica_li/market/lrfm_0212
                  <cal_date> <shop_id> <mean> <sd>
    Usage:        python mu2interval.py Mu2Interval --use-hadoop --src /user/erica_li/market/trans2shop.dat --dest /user/erica_li/market/lrfm_0212 --cal-date 20140202 --start-dt 20140129 --end-dt 20140202
    Attributes:   used partition for sorting
                  concatenate cal_date and shop_id for new key
    '''
    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d")) 
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, money = line.split(MIGO_SEPARATOR_LEVEL1)
            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

            if order_date <= datetime.strptime(self.cal_date, "%Y%m%d"):
                yield "{shop_id}{partition_sep}{member_id}{sep}{ordate}".format(shop_id=shop_id, partition_sep=self.sep, member_id=member_id, sep=MIGO_TMP_SEPARATOR, ordate=ordate), "{}_{}".format(member_id, ordate)
                self.count_success += 1
        except Exception as e:
            yield line, str(e)
            self.count_fail += 1

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):

        start = datetime.strptime(self.start_dt, "%Y%m%d")
        end = datetime.strptime(self.end_dt, "%Y%m%d")
        diff = end-start

        self.init_hadoop()
        self.init_reducer()

        #generated dict with whole cal_date
        mydict = {}
        for p in range(diff.days+1):
            cal_dt = start + td(days=p)
            cal_str_dt = datetime.strftime(cal_dt, "%Y-%m-%dT00:00:00")
            mydict[cal_str_dt] = []

        mydict[datetime.strftime(start, "%Y-%m-%dT00:00:00")] = [-999, -999, -999]

        def output(shop_id, tX2, tBarx, n, getdate):
            line = None
            if n > 0:
                mean = float(tBarx)/(n)
                line = "{}_{}\t{}\t{}".format(getdate, shop_id, mean, math.sqrt(float(tX2)/(n) - math.pow(mean, 2)))
                print >> stdout, line
            elif n == -999:
                line = "{}_{}\t{}\t{}".format(getdate, shop_id, -999, -999)
                print >> stdout, line
            else:
                self.count_fail += 1

        pre_shop_id = None
        pre_member_id = None
        pre_ordate = None

        num, x2, barx = 1, 0, 0

        for line in self.internal_reader((x[:-1] for x in stdin)):
            try:
                shop_id, others = line[0].split(self.sep)
                member_id, ordate = others.split(MIGO_TMP_SEPARATOR)

                sodate = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

                if pre_shop_id != None and pre_member_id != None:
                    if pre_shop_id == shop_id and pre_member_id == member_id:
                        yesterday = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        today = sodate
                        interval = (today - yesterday).days

                        x2 = math.pow(interval, 2)
                        barx = interval
                        num = 1

                        for key, value in mydict.items():
                            key_time = datetime.strptime(key, "%Y-%m-%dT00:00:00")
                            if today <= key_time:
                                if value and value[0] != -999:
                                    mydict[key] = [sum(x) for x in zip(value, [x2, barx, num])]
                                else:
                                    mydict[key] = [x2, barx, num]
                        
                    if pre_shop_id != shop_id:
                        for key, value in mydict.items():
                            if value:
                                x2, barx, num = value
                            else:
                                x2, barx, num = [-999, -999, -999]

                            output(pre_shop_id, x2, barx, num, key)

                        #empty all value for next shop in this reducer
                        mydict = {key: [] for key, value in mydict.items()}

                pre_shop_id = shop_id
                pre_member_id = member_id
                pre_ordate = ordate

                self.count_success += 1 
            except ValueError as e:
                self.count_fail += 1

        for key, value in mydict.items():
            if value:
                x2, barx, num = value
            else:
                x2, barx, num = [-999, -999, -999]
            output(pre_shop_id, x2, barx, num, key)

        self.end_reducer()

    def reducer(self, key, values):
        pass

if __name__ == "__main__": 
    luigi.run()
