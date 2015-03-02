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

class IndividualInterval(MigoLuigiHdfs):
    '''
    Objectives:    For each member, extract the perchasing intervals of his/her own as well as his/her first and latest order date.
    Input Fields:  <shop_id> <member_id> <order date> <money>
    Output Fields: <shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Source:        /user/erica_li/market/trans20.dat
                   <shop_id> <member_id> <order date> <money>
    Destination:   /user/erica_li/market/mu2_0212
    Usage:         python individualinterval.py IndividualInterval --use-hadoop --src /user/erica_li/market/trans2shop.dat --dest /user/erica_li/market/lrfm_0212 --cal-date 20140202
    Successor:     MuMeanStd; IndivisualIntervalFile
    '''

    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d")) 
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, money = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")
            if order_date <= datetime.strptime(self.cal_date, "%Y%m%d"):
                yield "{shop_id}{partition_sep}{member_id}{sep}{ordate}".format(shop_id=shop_id, partition_sep=self.sep, member_id=member_id, sep=MIGO_TMP_SEPARATOR, ordate=ordate), ordate
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

        def output(tmp_dict):
            line = None
            for k, v in tmp_dict.items():
                shop_id, getdate, member_id, min_dt = k.split("_")
                if v != -999:
                    line = "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, np.mean(v), len(v), min_dt, getdate)
                else:
                    line = "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, v, v, min_dt, getdate)
                print >> stdout, line

        mydict = {}

        pre_shop_id = None
        pre_member_id = None
        pre_ordate = None
        pre_min_date = None

        itvl = []

        for line in self.internal_reader((x[:-1] for x in stdin)):
            try:
                shop_id, others = line[0].split(self.sep)
                member_id, ordate = others.split(MIGO_TMP_SEPARATOR)

                sodate = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

                if pre_member_id == None:
                    pre_min_date = ordate
                    mydict[shop_id+"_"+ordate+"_"+member_id+"_"+ordate] = -999

                if pre_shop_id != None and pre_member_id != None:
                    if pre_member_id == member_id:
                        yesterday = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        today = sodate
                        interval = (today - yesterday).days

                        if interval > 1:
                            for i in range(1, diff.days):
                                lost_dt = yesterday + td(days=i) 
                                lost_str_dt = datetime.strftime(lost_dt, "%Y-%m-%dT00:00:00")
                                if itvl[:]:
                                    mydict[shop_id+"_"+lost_str_dt+"_"+member_id+"_"+pre_min_date] = itvl[:]

                        itvl.append(interval)
                        mydict[shop_id+"_"+ordate+"_"+member_id+"_"+pre_min_date] = itvl[:]

                    if pre_member_id != member_id:
#                        if len(itvl):
#                            print >> stdout, pre_shop_id, pre_member_id, np.mean(itvl), len(itvl), pre_min_date, pre_ordate
#                        else:
#                            print >> stdout, pre_shop_id, pre_member_id, -999, -999, pre_min_date, pre_ordate
                        itvl = []
                        pre_min_date = ordate

                        mydict[shop_id+"_"+ordate+"_"+member_id+"_"+pre_min_date] = -999

                pre_shop_id = shop_id
                pre_member_id = member_id
                pre_ordate = ordate

                self.count_success += 1 

            except ValueError as e:
                self.count_fail += 1

#        print >> stdout, pre_shop_id, pre_member_id, np.mean(itvl), len(itvl), pre_min_date, pre_ordate
        output(mydict)
        self.end_reducer()

    def reducer(self, key, values):
        pass

'''
                yield "{}\t{}\t{}\t{}\t{}".format(key.replace("_", MIGO_SEPARATOR_LEVEL1), np.mean(interval), len(interval), sorted_values[0], sorted_values[-1]),
                yield "{}\t{}\t{}\t{}\t{}".format(key.replace("_", MIGO_SEPARATOR_LEVEL1), MIGO_ERROR_NUMBER, MIGO_ERROR_NUMBER, sorted_values[0], sorted_values[-1]),
'''

if __name__ == "__main__": 
    luigi.run()
