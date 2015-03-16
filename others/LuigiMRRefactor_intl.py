#!/usr/bin/env python

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
    n_reduce_tasks = -1

    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d")) 
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return [CleanerHDFS(src) for src in self.src.split(",")]

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, money = line.split(MIGO_SEPARATOR_LEVEL1)
            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

            if (shop_id[-5:] != "^"+str(MIGO_ERROR_NUMBER)) and (member_id != str(MIGO_ERROR_NUMBER)):
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


class IndividualInterval(MigoLuigiHdfs):
    '''
    Objectives:    For each member, extract the perchasing intervals of his/her own as well as his/her first and latest order date.
    Source:        /user/erica_li/market/trans20.dat
                   <shop_id> <member_id> <order date> <money>
    Destination:   /user/erica_li/market/mu2_0212
                   <cal_date>_<shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Usage:         python individualinterval.py IndividualInterval --use-hadoop --src /user/erica_li/market/trans2shop.dat --dest /user/erica_li/market/lrfm_0212 --cal-date 20140202 --start-dt 20140129 --end-dt 20140202
    Successor:     MuMeanStd; IndivisualIntervalFile
    Attributes:    start_date and end_dt are neccessary
                   concatenate cal_date and shop_id for new key for the next stage
    '''

    n_reduce_tasks = -1
    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d")) 
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, money = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

            if (shop_id[-5:] != "^"+str(MIGO_ERROR_NUMBER)) and (member_id != str(MIGO_ERROR_NUMBER)):
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
                line = "{}_{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, np.mean(v), len(v), min_dt, max_dt)
            else:
                line = "{}_{}\t{}\t{}\t{}\t{}\t{}".format(getdate, shop_id, member_id, -999, -999, min_dt, max_dt)
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
                    output(shop_id, ordate, member_id, ordate, ordate, [])

                if pre_shop_id != None and pre_member_id != None:
                    if ((pre_member_id == member_id) and (pre_shop_id == shop_id)):
                        yesterday = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        today = sodate
                        interval = (today - yesterday).days

                        if interval > 1:
                            for i in range(1, interval):
                                lost_dt = yesterday + td(days=i) 
                                lost_str_dt = datetime.strftime(lost_dt, "%Y-%m-%dT00:00:00")
                                output(shop_id, lost_str_dt, member_id, pre_min_date, pre_ordate, itvl[:])

                        itvl.append(interval)
                        output(shop_id, ordate, member_id, pre_min_date, ordate, itvl[:])

                    if ((pre_member_id != member_id) or (pre_shop_id != shop_id)):
                        pre_ordate_dt = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
                        if pre_ordate_dt < end:
                            diff2 = end - pre_ordate_dt
                            for j in range(1, diff2.days+1):
                                supp_dt = pre_ordate_dt + td(days=j)
                                supp_str_dt = datetime.strftime(supp_dt, "%Y-%m-%dT00:00:00")
                                output(shop_id, supp_str_dt, pre_member_id, pre_min_date, pre_ordate, itvl[:])

                        itvl = []
                        pre_min_date = ordate
                        output(shop_id, ordate, member_id, pre_min_date, pre_min_date, [])

                pre_shop_id = shop_id
                pre_member_id = member_id
                pre_ordate = ordate

                self.count_success += 1 

            except ValueError as e:
                self.count_fail += 1

        #designed for the last member instead of new member in dataset
        if pre_ordate:
            final_dt = datetime.strptime(pre_ordate, "%Y-%m-%dT00:00:00")
            if final_dt < end:
                diff3 = end - final_dt
                for x in range(1, diff3.days+1):
                    supp_dt = final_dt + td(days=x)
                    supp_str_dt = datetime.strftime(supp_dt, "%Y-%m-%dT00:00:00")
                    output(pre_shop_id, supp_str_dt, pre_member_id, pre_min_date, pre_ordate, itvl[:])

        self.end_reducer()

    def reducer(self, key, values):
        pass


class MuMeanStd(MeanStd):
    '''
    Objectives: Calculate the mean and the standard deviation over the indivisual intervals of the members of shop_id.
    Input Fields: <shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Output Fields: <shop_id> <number of members who's indivisual interval is nonempty> <mean of indivisual intervals over the members of shop_id> <standard deviation of indivisual intervals over the members of shop_id> 
    Prerequisies: IndivisualInterval
    Successor: MuIntervalFile
    '''
    
    n_reduce_tasks = -1
    idx_key = luigi.Parameter(default="1")
    idx_value = luigi.Parameter(default="3")

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return IndividualInterval(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.src, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

class IndividualIntervalFile(DataMergerFile):
    '''
    Objectives: Prepare for DataMerger in next step.
    Input Fields: <shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Output Fields: <key> <value> (with key for DataMerger in next step)
    Prerequisites: IndivisualInterval
    Successor: MuInterval
    '''

    n_reduce_tasks = -1
    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return IndividualInterval(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.src, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

class MuIntervalFile(DataMergerFile):
    '''
    Objectives: Prepare for DataMerger in next step.
    Input Fields: <shop_id> <number of members who's indivisual interval is nonempty> <mean of indivisual intervals over the members of shop_id> <standard deviation of indivisual intervals over the members of shop_id> 
    Output Fields: <key> <value> (with key for DataMerger in next step)
    Prerequisites: MuMeanStd
    Successor: MuInterval    
    '''

    n_reduce_tasks = -1
    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return MuMeanStd(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.src, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

class MuInterval(DataMerger):
    '''
    Objectives: Merge the raw data from MuMeanStd and IndivisualInterval, with requested key and fields.
    Input Fields: (Output of MuIntervalFile); (Output of IndivisualIntervalFile)
    Output Fields: <shop_id> <mean of indivisual intervals over the members of shop_id> <standard deviation of indivisual intervals over the members of shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Prerequisites: MuIntervalFile; IndivisualIntervalFile
    Successor: MuFile
    '''

    n_reduce_tasks = -1
    src = luigi.Parameter(default="/user/athena/able/data_prepare_member/201208*;/user/athena/able/data_prepare_member/201208*")

    key = luigi.Parameter(default="1;1")
    value = luigi.Parameter(default="3,4;2,3,4,5,6")

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        self.log("self.folders=%s" %self.folders)
        self.log("self.idx_index=%s" %self.idx_index)

        return [MuIntervalFile(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.tmp_folders[0], dest=self.folders[0], idx_key=self.tmp_keys[0], idx_value=self.tmp_values[0], idx_index=self.partition_subkey_sep.join(self.idx_index[0]), partition_key_sep=self.partition_subkey_sep, partition_value_sep=self.partition_value_pos, partition_sep=self.partition_sep, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt), IndividualIntervalFile(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.tmp_folders[1], dest=self.folders[1], idx_key=self.tmp_keys[1], idx_value=self.tmp_values[1], idx_index=self.partition_subkey_sep.join(self.idx_index[1]), partition_key_sep=self.partition_subkey_sep, partition_value_sep=self.partition_value_pos, partition_sep=self.partition_sep, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)]

class Mu2File(DataMergerFile):
    '''
    Objectives: Prepare for DataMerger in next step.
    Input Fields: <shop_id> <sum(interval**2)> <sum over all intervals> <number of intervals>
    Output Fields: <key> <value> (with key for DataMerger in next step)
    Prerequisites: Mu2Interval
    Successor: MergeInterval
    '''

    n_reduce_tasks = -1
    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return Mu2Interval(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.src, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

class MuFile(DataMergerFile):
    '''
    Objectives: Prepare for DataMerger in next step.
    Input Fields: <shop_id> <mean of indivisual intervals over the members of shop_id> <standard deviation of indivisual intervals over the members of shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Output Fields: <key> <value> (with key for DataMerger in next step)
    Prerequisites: MuInterval
    Successor: MergeInterval   
    '''

    n_reduce_tasks = -1
    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return MuInterval(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src="{src};{src}".format(src=self.src), cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

class MergeInterval(DataMerger):
    '''
    Objectives: Merge the raw data from Mu2Interval (from MuMeanStd and IndivisualInterval) and MuInterval
    Input Fileds: (Output of Mu2File); (Output of MuFile)
    Output Fields: <shop_id> <sum(interval**2)> <sum over all intervals> <mean of indivisual intervals over the members of shop_id> <standard deviation of indivisual intervals over the members of shop_id> <member_id> <mean of intervals> <number of intervals> <first order date> <last order date>
    Prerequisites: Mu2File; MuFile 
    Successor: AdjustedInterval      
    '''

    n_reduce_tasks = -1
    src = luigi.Parameter(default="/user/athena/able/data_prepare_member/201208*;/user/athena/able/data_prepare_member/201208*")

    key = luigi.Parameter(default="1;1")
    value = luigi.Parameter(default="2,3;2,3,4,5,6,7,8")

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        self.log("self.folders=%s" %self.folders)
        self.log("self.idx_index=%s" %self.idx_index)

        return [Mu2File(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.tmp_folders[0], dest=self.folders[0], idx_key=self.tmp_keys[0], idx_value=self.tmp_values[0], idx_index=self.partition_subkey_sep.join(self.idx_index[0]), partition_key_sep=self.partition_subkey_sep, partition_value_sep=self.partition_value_pos, partition_sep=self.partition_sep, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt),MuFile(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.tmp_folders[1], dest=self.folders[1], idx_key=self.tmp_keys[1], idx_value=self.tmp_values[1], idx_index=self.partition_subkey_sep.join(self.idx_index[1]), partition_key_sep=self.partition_subkey_sep, partition_value_sep=self.partition_value_pos, partition_sep=self.partition_sep, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)]

class AdjustedInterval(MigoLuigiHdfs):
    '''
         JobTask: Adjusted Purchasing Interval
       Objective: Calculate the Adjusted Purchasing Interval for Individual based on Group Purchasiing Interval and Individual Purchasing Interval
          Author: RungChi Chen
    CreationDate: 2015/01/20
          Source: Schema
                <shop_id> <mu2_mean> <mu2_std> <mu_mean> <mu_std> <member_id> <mean> <repeat_count> <first_transaction_date> <last_transaction_date>
                kgsupermarket^C001  3.80526268089   3.0464552347    4.87404931082   3.16983471432   249105  0 -999  2014-08-08T00:00:00 2014-08-08T00:00:00
     Destination: Adjusted Purchasing Interval (/user/athena/adjusted_interval)
                     <shop_id>\t<member_id>\t<interval>
           Usage: python interval.py AdjustedInterval --use-hadoop --keep-temp --key "1;1" --value "2,3;2,3,4,5,6,7,8" --src "/user/athena/data_prepare_member/*/;/user/athena/data_prepare_member/*/" --dest adjusted_interval/20140810 --cal-date 20140810
                  python interval_testing.py AdjustedInterval --use-hadoop --key "1;1" --value "2,3;2,3,4,5,6,7,8" --src "/user/athena/able/data_prepare_member/201208*;/user/athena/able/data_prepare_member/201208*" --dest /user/erica_li/market/ind_2012 --cal-date 20120831 --start-dt 20120801 --end-dt 20120831
    '''

    n_reduce_tasks = -1
    src = luigi.Parameter(default="/user/athena/able/data_prepare_member/201208*;/user/athena/able/data_prepare_member/201208*")

    key = luigi.Parameter(default="1;1")
    value = luigi.Parameter(default="2,3;2,3,4,5,6,7,8")

    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    def requires(self):
        return MergeInterval(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, src=self.src, key=self.key, value=self.value, cal_date=self.cal_date, start_dt=self.start_dt, end_dt=self.end_dt)

    def mapper(self, line):
        try:
            shop_id, mu2, ss2, mu, ss, member_id, mean, repeat_count, first_transaction_date, last_transaction_date = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            if member_id != MIGO_STAMP_FOR_EMPTY_RECORD:
                if ss2 == MIGO_STAMP_FOR_EMPTY_RECORD or ss == MIGO_STAMP_FOR_EMPTY_RECORD:
                    yield "{shop_id}\t{member_id}\t{gap_mean_adj}\t{first}\t{last}".format(shop_id=shop_id,
                                                                                       member_id=member_id,
                                                                                       gap_mean_adj=MIGO_ERROR_NUMBER,
                                                                                       first=first_transaction_date,
                                                                                       last=last_transaction_date),
                elif float(ss) == 0 or float(ss2) == 0:
                    yield "{shop_id}\t{member_id}\t{gap_mean_adj}\t{first}\t{last}".format(shop_id=shop_id,
                                                                                       member_id=member_id,
                                                                                       gap_mean_adj=MIGO_ERROR_NUMBER,
                                                                                       first=first_transaction_date,
                                                                                       last=last_transaction_date),
                else:
                    mu = float(mu)
                    ss = float(ss)**2
                    alpha = mu**2/ss + 2
                    theta = ss/(mu*(mu**2 + ss))

                    ss2 = float(ss2)**2
                    mu2 = float(mu2)

                    mean = float(mean)
                    repeat_count = float(repeat_count)
                    k = mu2**2 / ss2
                    w1 = repeat_count/(k*repeat_count + alpha -1)
                    w2 = 1/(k*repeat_count+alpha-1)
                    gap_mean_adj = k * (w1 * mean + w2/theta)
                    if repeat_count == MIGO_ERROR_NUMBER:
                        gap_mean_adj = k/((alpha-1)*theta)

                    yield "{shop_id}\t{member_id}\t{gap_mean_adj}\t{first}\t{last}".format(shop_id=shop_id, 
                                                                                           member_id=member_id,
                                                                                           gap_mean_adj=gap_mean_adj,
                                                                                           first=first_transaction_date, 
                                                                                           last=last_transaction_date),

            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

if __name__ == "__main__":
    luigi.run()