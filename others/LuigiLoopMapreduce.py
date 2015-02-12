#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

from datetime import datetime
from datetime import date, timedelta as td

import decimal
import luigi, luigi.hdfs, luigi.hadoop

class LRFM(MigoLuigiHdfs):
    '''
    Job Task:     lrfm for data supplement
    Objective:    optimazed data supplement
    Author:       Erica L Li
    Created Date: 2015.02.12
    Source:       /user/erica_li/market/trans20.dat
    Destination:  /user/erica_li/market/report
    Usage:        python lrfm_beta.py LRFM --use-hadoop --src /user/erica_li/market/trans20.dat --dest /user/erica_li/market/lrfm_0212
    Attributes:   
    '''
    start_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d")) 
    end_dt = luigi.Parameter(default=datetime.today().strftime("%Y%m%d"))

    n_reduce_tasks = 1

    def mapper(self, line):
        try:
            shop_id, member_id, ordate, amount = line.split(MIGO_SEPARATOR_LEVEL1)
            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")
            if order_date <= datetime.strptime(self.cal_date, "%Y%m%d"):
                yield "{}{}{}".format(shop_id, MIGO_TMP_SEPARATOR, member_id), "{}{}{}".format(ordate, MIGO_TMP_SEPARATOR, amount)
                self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def reducer(self, key, values):


#        dt_min = None
#        dt_max = None

#        for val in values:
#            ordate, amount = val.split(MIGO_TMP_SEPARATOR)
#            order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")

#            if dt_min == None:
#                dt_min = dt_max = order_date
#            elif order_date < dt_min:
#               dt_min = order_date
#            elif order_date > dt_max:
#                dt_max = order_date

#        yield "{}_{}".format(dt_min, dt_max),

        start = datetime.strptime(self.start_dt, "%Y%m%d")
        end = datetime.strptime(self.end_dt, "%Y%m%d")
        diff = end-start
        myvalue = list(values)

        for i in range(diff.days + 1):
            now_dt = start + td(days=i)
            #print datetime.strftime(now_dt, "%Y-%m-%dT00:00:00")

            day_min = datetime.max
            day_max = datetime.min
        
            order_f = 0
            order_m = 0

            shop_id, member_id = key.split(MIGO_TMP_SEPARATOR)    
            try:
                for val in myvalue:
                    try:    
                        ordate, amount = val.split(MIGO_TMP_SEPARATOR)            
                        order_date = datetime.strptime(ordate, "%Y-%m-%dT00:00:00")
                        if order_date <= now_dt:  

                            if order_date < day_min : 
                                day_min = order_date
                            if order_date > day_max :
                                day_max = order_date

                            order_f += 1
                            order_m += decimal.Decimal(amount)
                            self.count_success += 1
                    except ValueError as e:
                        yield "%s: %s - %s" %(MIGO_STAMP_FOR_ERROR_RECORD, key + "_" + val, e),
                        self.count_fail += 1
        
                #L
                output_l = (day_max - day_min).days
                #R
                output_r = (now_dt - day_max).days
        
                if order_m > 0:
                    yield now_dt, shop_id, member_id, output_l, output_r, order_f, order_m
            except ValueError as e:
                yield "%s: %s - %s" %(MIGO_STAMP_FOR_ERROR_RECORD, key, e),

if __name__ == "__main__": 
    luigi.run()

