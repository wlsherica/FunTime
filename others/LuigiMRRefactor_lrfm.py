#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

from datetime import datetime

import decimal
import luigi

class SortData(MigoLuigiHdfs):

    n_reduce_tasks = -1
    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, amount = line.split(MIGO_SEPARATOR_LEVEL1)
            if (shop_id[-5:] != "^-999") and (member_id != "-999"):
                yield "{}{}{}{}{}".format(shop_id, MIGO_TMP_SEPARATOR, member_id, self.sep, ordate), amount
                self.count_success += 1
        except:
            self.count_fail += 1
            pass
    
    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):        
        pre_shop_id = None
        pre_member_id = None
        integrate_f = 0
        integrate_m = 0

        for line in self.internal_reader((x[:-1] for x in stdin)):
            try:
                shop_member, ordate = line[0].split(self.sep)
                shop_id, member_id = shop_member.split(MIGO_TMP_SEPARATOR)
                amount = line[1]

                if not (shop_id == pre_shop_id and member_id == pre_member_id):
                    integrate_f = 0
                    integrate_m = 0

                integrate_f += 1
                integrate_m += decimal.Decimal(amount)

                print >> stdout, "{}{}{}{}{}\t{}{}{}".format(shop_id, MIGO_TMP_SEPARATOR, member_id, self.sep, ordate, integrate_f, MIGO_TMP_SEPARATOR, integrate_m)
                pre_shop_id = shop_id
                pre_member_id = member_id

            except:
                raise

    def reducer(self, key, values):
        pass

class LRFM(MigoLuigiHdfs):
    '''
        Job Name: new_lrfm.py
      Objectives: Load the data just one time and calculate the lrfm for all dates.
          Author: Jeff Jiang, Erica Li
    Created Date: 2015-02-17
    Input Schema: <shop_id><member_id><ordate><amount>
   Output Schema: <shop_id><member_id><output_l><output_r><output_f><output_m> 
           Usage: python new_lrfm.py LRFM --end-dt <end_date> --use-hadoop --src <src_path>  --dest <dest_path> 
                  e.g. python new_lrfm.py LRFM --end-dt 20140222 --use-hadoop --src /user/erica_li/market/trans20.dat --dest /user/jeff_jiang/happy_new_year
                  e.g. python lrfm_testing.py LRFM --end-dt 2012-12-31 --use-hadoop --src /user/athena/able/data_prepare_member/2012* --dest /user/erica_li/market/lrfm_test

    '''

    n_reduce_tasks = -1
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))
    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)
     
    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def start(self):
        self.end_dt = datetime.strptime(self.end_dt, "%Y%m%d")
        self.pre_shop_id = None
        self.pre_member_id = None
        self.integrate_f = 0
        self.integrate_m = 0

    def requires(self):
        return SortData(use_hadoop = True, src = self.src)
    
    def mapper(self, line):            
        try:
            info = line.split(MIGO_SEPARATOR_LEVEL1)
            shop_member, ordate = info[0].split(self.sep)

            shop_id, member_id = shop_member.split(MIGO_TMP_SEPARATOR)
            integrate_f, integrate_m  = info[1].split(MIGO_TMP_SEPARATOR)

            yield "{}{}{}{}{}".format(shop_id, MIGO_TMP_SEPARATOR, member_id, self.sep, ordate), "{}{}{}".format(integrate_f, MIGO_TMP_SEPARATOR, integrate_m)

            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        start_flag = True
        pre_shop_id = None        
        pre_member_id = None
        pre_ordate = datetime.strptime("2011-01-01T00:00:00", "%Y-%m-%dT00:00:00") 
        day_min = None
        pre_integrate_f = 0
        pre_integrate_m = decimal.Decimal(0.0)
        
        for line in self.internal_reader((x[:-1] for x in stdin)):
            try:
                shop_member, ordate = line[0].split(self.sep)
                shop_id, member_id = shop_member.split(MIGO_TMP_SEPARATOR)
                integrate_f, integrate_m  = line[1].split(MIGO_TMP_SEPARATOR)

                if not (shop_id == pre_shop_id and member_id == pre_member_id):
                    if start_flag:
                        start_flag = False
                    else:    
                        for i in range((self.end_dt - pre_ordate).days +1):
                            try:
                                caldate = pre_ordate + timedelta(days = i)
                                
                                print >> stdout, "{}_{}\t{}\t{}\t{}\t{}\t{}".format(datetime.strftime(caldate, "%Y-%m-%dT00:00:00"), pre_shop_id, pre_member_id, (pre_ordate - day_min).days, (caldate - pre_ordate).days, pre_integrate_f, pre_integrate_m)
                            except:
                                pass

                    day_min = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")
                    pre_ordate = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")
                    pre_integrate_f = 0
                    pre_integrate_m = decimal.Decimal(0.0)                    
                    
                now = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")                
                diff_days = (now -pre_ordate).days

                for i in range(diff_days):
                    caldate = pre_ordate + timedelta(days = i)
                    output_l = (pre_ordate - day_min).days
                    output_r = (caldate - pre_ordate).days
                    output_f = pre_integrate_f
                    output_m = pre_integrate_m
                    
                    print >> stdout, "{}_{}\t{}\t{}\t{}\t{}\t{}".format(datetime.strftime(caldate, "%Y-%m-%dT00:00:00"), pre_shop_id, pre_member_id, output_l, output_r, output_f, output_m)
                pre_shop_id = shop_id
                pre_member_id = member_id
                pre_ordate = now
                pre_integrate_f = int(integrate_f, 10)
                pre_integrate_m = decimal.Decimal(integrate_m)
                 
            except:
                pass

        for i in range((self.end_dt - pre_ordate).days +1):
            try: 
                caldate = pre_ordate + timedelta(days = i)
                print >> stdout, "{}_{}\t{}\t{}\t{}\t{}\t{}".format(datetime.strftime(caldate, "%Y-%m-%dT00:00:00"), pre_shop_id, pre_member_id, (pre_ordate - day_min).days, (caldate - pre_ordate).days, pre_integrate_f, pre_integrate_m)
            except:          
                pass

    def reducer(self, key, values):
        pass

if __name__ == "__main__": 
    luigi.run()
