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

        def output(shop_id, getdate, member_id, min_dt, max_dt, v):
            line = None
            if v:
                line = "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, np.mean(v), len(v), min_dt, max_dt)
            else:
                line = "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, -999, -999, min_dt, max_dt)
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
                    #print >> stdout, "FIRST LINE:", shop_id+"_"+ordate+"_"+member_id+"_"+ordate+"_"+ordate, []
                    output(shop_id, ordate, member_id, ordate, ordate, [])

                if pre_shop_id != None and pre_member_id != None:
                    if pre_member_id == member_id:
                        yesterday = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        today = sodate
                        interval = (today - yesterday).days

                        if interval > 1:
                            for i in range(1, interval):
                                lost_dt = yesterday + td(days=i) 
                                lost_str_dt = datetime.strftime(lost_dt, "%Y-%m-%dT00:00:00")
                                #print >> stdout, "2ND LINE:", shop_id+"_"+lost_str_dt+"_"+member_id+"_"+pre_min_date+"_"+pre_ordate, str(itvl[:])
                                output(shop_id, lost_str_dt, member_id, pre_min_date, pre_ordate, itvl[:])

                        itvl.append(interval)
                        #print >> stdout, "3RD LINE:", shop_id+"_"+ordate+"_"+member_id+"_"+pre_min_date+"_"+ordate, str(itvl[:])
                        output(shop_id, ordate, member_id, pre_min_date, ordate, itvl[:])

                    if pre_member_id != member_id:
                        pre_ordate_dt = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        if pre_ordate_dt < end:
                            diff2 = end - pre_ordate_dt
                            for j in range(1, diff2.days+1):
                                supp_dt = pre_ordate_dt + td(days=j)
                                supp_str_dt = datetime.strftime(supp_dt, "%Y-%m-%dT00:00:00")
                                #print >> stdout, "4TH LINE:", shop_id+"_"+supp_str_dt+"_"+pre_member_id+"_"+pre_min_date+"_"+pre_ordate, str(itvl[:])
                                output(shop_id, supp_str_dt, pre_member_id, pre_min_date, pre_ordate, itvl[:])

                        itvl = []
                        pre_min_date = ordate

                        #print >> stdout, "LAST LINE:", shop_id+"_"+ordate+"_"+member_id+"_"+pre_min_date+"_"+pre_min_date, []
                        output(shop_id, ordate, member_id, pre_min_date, pre_min_date, [])

                pre_shop_id = shop_id
                pre_member_id = member_id
                pre_ordate = ordate

                self.count_success += 1 

            except ValueError as e:
                self.count_fail += 1

        self.end_reducer()

    def reducer(self, key, values):
        pass

if __name__ == "__main__": 
    luigi.run()
