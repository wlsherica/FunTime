#!/usr/bin/env python

import sys
sys.path.append("/data/migo/athena/lib")
sys.path.append("/data/migo/athena/util")

import re, math, getpass, time

from itertools import groupby
from datetime import datetime

from athena_luigi import MigoLuigiHdfs, CleanerHDFS
from athena_mr_string import stripQuote
from athena_variable import *

import commands
import luigi, luigi.hdfs, luigi.hadoop

class Mean(MigoLuigiHdfs):
    '''
       Job Task: Mean
     Objectives: Calculate Mean for Pareto Analysis
         Author: RungChi Chen
    CreatedDate: 2015/01/11
         Source: None
    Destination: None
          Usage: Can't use it alone! You should use ParetoAnalysis Module!

    Attributes:
    -----------
    idx_key: luigi.Parameter --> The index of key, it support mulitple key index. Use comma(,) to split it
    idx_value: luigi.IntParameter --> The index of value (This class will calculate the mean based on this index)
    fileter_value: luigi.Parameter --> What kind of value can we drop?
    '''
    idx_key = luigi.Parameter(default="1")
    idx_value = luigi.IntParameter(default=4)
    filter_value = luigi.Parameter(default=None)

    def mapper(self, line):
        def get_key(infos):
            key = []
            for idx in self.idx_key.split(","):
                key.append(infos[int(idx)-1])
            return MIGO_TMP_SEPARATOR.join(key)

        try:
            infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            if self.filter_value == None:
                yield get_key(infos), (float(infos[self.idx_value-1]), 1)
            else:
                if infos[idx_value] != self.filter_value:
                    yield get_key(infos), (float(infos[self.idx_value-1]), 1)
                    self.count_success += 1
        except Exception as e:
            yield MIGO_STAMP_FOR_ERROR_RECORD, e.message
            self.count_fail += 1

    def sum(self, values):
        num_sum, num = 0, 0
        for value in values:
            num_sum += float(value[0])
            num += value[1]
        return num_sum, num

    def combiner(self, key, values):
        yield key, self.sum(values)

    def reducer(self, key, values):
        try:
            all, num = self.sum(values)
            yield key, "!%f" %(all/num)
            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

class HighMean(MigoLuigiHdfs):
    '''
       Job Name: High Mean
     Objectives: Calculate the "High Mean" for Pareto Analysis. High mean is that calculate the mean based on those value which are greater than mean
         Author: RungChi Chen
    CreatedDate: 2015/01/11
         Source: None
    Destination: None
          Usage: You can NOT use it directedly. You should use ParetoAnalysis Module instead of it

    Attributes:
    -----------
    sep: luigi.Parameter --> The character is a separator for the partition of MR

    idx_key: luigi.Parameter --> The index of key, it support mulitple key index. Use comma(,) to split it
    idx_value: luigi.IntParameter --> The index of value (This class will calculate the mean based on this index)
    fileter_value: luigi.Parameter --> What kind of value can we drop?

    mean_folder: luigi.Parameter --> The destination to the mean based on those value which are greater than mean
    '''

    sep = luigi.Parameter(default="|")

    idx_key = luigi.Parameter(default=1)
    idx_value = luigi.IntParameter(default=4)
    filter_value = luigi.Parameter(default=None)
    mean_folder = luigi.Parameter(default="")

    def start(self):
        if self.mean_folder == "":
            self.mean_folder = self.tmp_path("pareto_highmean_mean")

#    def extra_confs(self):
#        return ["mapreduce.map.output.key.field.separator=%s" %self.sep, "mapreduce.partition.keypartitioner.options=-k1,1"]

    def requires(self):
        return [Mean(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, idx_key=self.idx_key, idx_value=self.idx_value, src=self.src, filter_value=self.filter_value, dest=self.mean_folder), CleanerHDFS(self.src)]

    def init_mapper(self):
        if self.hdfsClient.exists(self.mean_folder):
            cmd = "hadoop fs -text {}/*".format(self.mean_folder)
            status, output = commands.getstatusoutput(cmd)

            self.mean_data = {}
            for line in output.split("\n"):
                shop_id, mean_raw = line.split(MIGO_SEPARATOR_LEVEL1)

                if mean_raw.count("!") == 1 and mean_raw.find("!") == 0:
                    mean = float(mean_raw[1:])
                    self.mean_data[shop_id] = mean
              
    def mapper(self, line):
        try:
            infos = line.split(MIGO_SEPARATOR_LEVEL1)
            shop_id, member_id, ordate, money = infos
            if shop_id in self.mean_data and float(money) >= self.mean_data[shop_id]:
                yield "{}".format(shop_id), float(money)

            self.count_success += 1
        except Exception as e:
            yield MIGO_STAMP_FOR_ERROR_RECORD, e.message
            self.count_fail += 1

    def sum_n(self, values):
        total, n = 0, 0
        for val in values:
            total += float(val)
            n += 1
        return total, n

    def reducer(self, key, values):
        try:
            sum_info, n_info = self.sum_n(values)
            if float(n_info) > 0:
                yield "{}".format(key), "*{}".format(float(sum_info)/float(n_info))
            else:
                yield "ZERO: ", key, values
            self.count_success += 1
        except Exception as e:
            yield "DAMN: ", e.message
            self.count_fail += 1



class ParetoAnalysis(MigoLuigiHdfs):
    '''
       Job Name: ParetoAnalysis Module
     Objectives: Give the Tag based on the mean and high-mean. 
                 1. If the value is greater than high-value, this column will be tagged as High(H).
                 2. If the value is greater than mean but less/equal than high-mean, this column will be tagged as Medium(M).
                 3. The remaining parts will be tagged as Low(L)
         Author: RungChi Chen, Erica Li
    CreatedDate: 2015/01/11, 2015/03/17
         Source: /user/rungchi/data_prepare_member
                 [shop_id]  [member_id] [transaction_date]  [amount]
    Destination: /user/rungchi/pareto_analysis
                 [shop_id]  [member_id] [transaction_date]  [amount]    [TAG]
          Usage: python athena_math.py ParetoAnalysis --use-hadoop --src /user/rungchi/data_prepare_member --dest /user/rungchi/pareto_analysis --idx-value 1 --idx-value 4 --tagging-name Lifetime

    Attributes:
    -----------
    sep: luigi.Parameter --> The character is a separator for the partition of MR

    idx_key: luigi.Parameter --> The index of key, it support mulitple key index. Use comma(,) to split it
    idx_value: luigi.IntParameter --> The index of value (This class will calculate the mean based on this index)
    tagging_name: luigi.Parameter --> The customized tag name setting
    fileter_value: luigi.Parameter --> What kind of value can we drop?

    mean_folder: luigi.Parameter --> The destination to the mean based on those value which are greater than mean

    Methods:
    -----------
    requires(self):
        This Luigi class needs THREE Luigi tasks. One is "Mean"; The other is "HighMean"; Another is HDFS source file. Then, you should give the following luigi.parameters for them.
        CleanerHdfs:
            src: HDFS File Path

        Mean:
              use_hadoop: Use HDFS or not
               keep_temp: Before running, delete the all dest and temporary files or not
                 idx_key: The index of Key Column (Start From: 1)
               idx_value: The index of Value Column (Start From: 1)
                     src: Source of HDFS File Path
                    dest: Destination of HDFS File Path
            filter_value: Cut off what value is (Maybe -999)

        HighMean:
              use_hadoop: Use HDFS or not
               keep_temp: Before running, delete the all dest and temporary files or not
                 idx_key: The index of Key Column (Start From: 1)
               idx_value: The index of Value Column (Start From: 1)
                     src: Source of HDFS File Path
                    dest: Destination of HDFS File Path
            filter_value: Cut off what value is (Maybe -999)
             mean_folder: It SHOULD be the same with "the dest of Mean"
    '''

    sep = luigi.Parameter(default="|")

    idx_key = luigi.Parameter(default="1")
    idx_value = luigi.IntParameter(default=4)
    tagging_name = luigi.Parameter(default="X")
    filter_value = luigi.Parameter(default=None)

#    dest_mean = luigi.Parameter(default=None)
#    dest_highmea luigi.Parameter(default=None)

    def start(self):
        self.tmp_mean_folder = self.tmp_path("pareto_mean")
        self.tmp_high_mean_folder = self.tmp_path("pareto_highmean")

    def requires(self):
        return [HighMean(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, idx_key=self.idx_key, idx_value=self.idx_value, src=self.src, dest=self.tmp_high_mean_folder, filter_value=self.filter_value, mean_folder=self.tmp_mean_folder),
                Mean(use_hadoop=self.use_hadoop, keep_temp=self.keep_temp, idx_key=self.idx_key, idx_value=self.idx_value, src=self.src, dest=self.tmp_mean_folder, filter_value=self.filter_value), CleanerHDFS(self.src)]

    def init_mapper(self):
        if self.hdfsClient.exists(self.tmp_mean_folder):
            cmd = "hadoop fs -text {}/*".format(self.tmp_mean_folder)
            status, output = commands.getstatusoutput(cmd)

            self.mean_dict = {}
            #key, "!%f" %(all/num)
            for line in output.split("\n"):
                shop_id, mean_raw = line.split(MIGO_SEPARATOR_LEVEL1)
                
                if mean_raw.count("!") == 1 and mean_raw.find("!") == 0:
                    mean = float(mean_raw[1:])
                    self.mean_dict[shop_id] = mean

        if self.hdfsClient.exists(self.tmp_high_mean_folder):
            cmd = "hadoop fs -text {}/*".format(self.tmp_high_mean_folder)
            status, output = commands.getstatusoutput(cmd)

            self.highmean_dict = {}
            #"{}\t*{}".format(shop_id, s/n)
            for line in output.split("\n"):
                if len(line.strip()) > 0:
                    shop_id, highmean_raw= line.split(MIGO_SEPARATOR_LEVEL1)

                    if highmean_raw.count("*") == 1 and highmean_raw.find("*") == 0:
                        high_mean = float(highmean_raw[1:])
                        self.highmean_dict[shop_id] = high_mean

#    def extra_confs(self):
#        return ["mapreduce.map.output.key.field.separator=%s" %self.sep, "mapreduce.partition.keypartitioner.options=-k1,1"]


    def mapper(self, line):
        try:  
            infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            if len(infos) == 4:
                shop_id, member_id, ordate, money = infos

                if shop_id in self.highmean_dict and shop_id in self.mean_dict:
                    v = float(infos[self.idx_value-1])

                    if v >= self.highmean_dict[shop_id]:
                        #output(value.replace(MIGO_MR_REPLACE_FOR_TAB, MIGO_SEPARATOR_LEVEL1), "{}{}".format(self.tagging_name, MIGO_PARETO_H))
                        yield "{}\t{}\t{}\t{}".format(shop_id, member_id, ordate, money), "{}{}".format(self.tagging_name, MIGO_PARETO_H)
                    elif v >= self.mean_dict[shop_id]:
                        #output(value.replace(MIGO_MR_REPLACE_FOR_TAB, MIGO_SEPARATOR_LEVEL1), "{}{}".format(self.tagging_name, MIGO_PARETO_M))
                        yield "{}\t{}\t{}\t{}".format(shop_id, member_id, ordate, money), "{}{}".format(self.tagging_name, MIGO_PARETO_M)
                    else:
                        #output(value.replace(MIGO_MR_REPLACE_FOR_TAB, MIGO_SEPARATOR_LEVEL1), "{}{}".format(self.tagging_name, MIGO_PARETO_L))
                        yield "{}\t{}\t{}\t{}".format(shop_id, member_id, ordate, money), "{}{}".format(self.tagging_name, MIGO_PARETO_L)

            self.count_success += 1
        except Exception as e:
            yield MIGO_STAMP_FOR_ERROR_RECORD, e.message
            self.count_fail += 1


class PrePercentile(MigoLuigiHdfs):
    '''
        Job Task: PrePercentile
      Objectives: Can't use it along!!!!
          Author: RungChi
    CreationDate: 2015/01/12
          Source: None
                  Examples
                  -----------------------
                  kgsupermarket^C001  68574821    11.6635593817
                  kgsupermarket^C001  68437238    24.6808852749
                  kgsupermarket^C001  01860   11.7479026555
                  ...
     Destination: None
           Usage: None

    Attributes
    -------------------
         idx_key: luigi.Parameter --> The index of key column (If multiple keys, we can use COMMA to join them)
       idx_value: luigi.IntParameter --> The index of value column
    map_key_sepa: luigi.Parameter --> The separator is for partitioner 
    '''
    idx_key = luigi.IntParameter()
    idx_value = luigi.IntParameter()

    map_key_sepa ="#"
    partitioned = True
    comparator = False
    cnt = 0

    def extra_confs(self):
        confs = []

        if self.partitioned:
            confs = ["mapreduce.map.output.key.field.separator=%s" % self.map_key_sepa, "mapreduce.partition.keypartitioner.options=-k1,1"]

        if self.comparator:
            confs.append("mapreduce.map.output.key.field.separator=%s" % self.map_key_sepa)
            confs.append("mapreduce.partition.keycomparator.options=-k2,2n")
            confs.append("mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator")

        return confs

    def mapper(self, line):
        try:
            infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
            key = infos[self.idx_key-1]
            value = infos[self.idx_value-1]

            yield "%s#%030.4f" % (key, float(value)), 1
            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def reducer(self, key, values):
        for value in values:
            self.cnt += 1

            yield "{}#{}".format(key, self.cnt),
            self.count_success += 1

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        pshop_id = None
        mx_cnt = 0

        for key, values in groupby(inputs, key=lambda x: repr(x[0])):
            shop_id, value = key.split(self.map_key_sepa)
            shop_id = eval(shop_id + "'")

            if pshop_id != None and pshop_id != shop_id:
                yield "{}#!#{}".format(pshop_id, mx_cnt),

                mx_cnt = 0
                self.cnt = 0

            n = 0
            for output in reducer(eval(key), (v[1] for v in values)):
                n += 1

                yield output

            pshop_id = shop_id
            mx_cnt += n

        if pshop_id != None:
            yield "{}#!#{}".format(pshop_id, mx_cnt),

        self._flush_batch_incr_counter()
        self.end_reducer()

class Percentile(MigoLuigiHdfs):
    '''
        Job Task: Percentile
      Objectives: Calculate the percentile for value list
          Author: RungChi Chen
     CreatedDate: 2015/01/12
          Source: There are two columns more
     Destination: HDFS File Destinatino

    Attributes
    --------------------
      idx_key: The Index of Key Column (Start from 1)
    idx_value: The Index of Value Column (Start from 1), The percentile calculation will be based on it.
          idx: Which percentile part do you want? (Ex, 50, this class will return the 50 percentile to you)
    '''

    sep = luigi.Parameter(default=MIGO_MR_REPLACE_FOR_TAB)

    idx_key = luigi.IntParameter()
    idx_value = luigi.IntParameter()
    idx = luigi.Parameter(default="50")

    is_ceil = luigi.BooleanParameter(default=False)

    def extra_confs(self):
        return ["mapreduce.map.output.key.field.separator={}".format(self.sep), "mapreduce.partition.keypartitioner.options=-k1,1"]

    def start(self):
        self.dest_tmp = self.tmp_path(self.src)

    def requires(self):
        return PrePercentile(src=self.src, use_hadoop=self.use_hadoop, dest=self.dest_tmp, idx_key=self.idx_key, idx_value=self.idx_value, keep_temp=self.keep_temp)

    def mapper(self, line):
        try:
            shop_id, value, idx = line.strip().split("#")
            yield "{}{}{}".format(shop_id, self.sep, value), idx
            self.count_success += 1
        except Exception as e:
            self.count_fail += 1

    def reducer(self):
        pass

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        self.init_hadoop()
        self.init_reducer()

        pct_dict = {}
        for line in self.internal_reader((line[:-1] for line in stdin)):
            #shop_id, val, idx = re.split("[_\s]", line)
            shop_id, val = line[0].split(self.sep)
            idx = int(line[1])

            if val == "!":
                pct_dict = {}

                for i in range(1, 101):
                    pct_idx = int(round(i*(idx - 1)/100 + 1))
                    pct_dict[i] = pct_idx
            else:
                for percentile in self.idx.split(","):
                    percentile = int(percentile)
                    if percentile in pct_dict:
                        if pct_dict[percentile] == idx:
                            v = float(val)
                            if self.is_ceil:
                               v = math.ceil(v)

                            print >> stdout, "{}\t{}".format(shop_id, v)
                    else:
                        print >> stdout, "Wrong Percentile Number - {}. Are you kidding me?".format(self.idx)

        self.end_reducer()

class PearsonCC(MigoLuigiHdfs):
    keep_temp = luigi.BooleanParameter(default=False, config_path={"section": "pearson", "name": "keep_temp"})

    idx_key = luigi.IntParameter(default=MIGO_ERROR_NUMBER, config_path={"section": "pearson", "name": "idx_key"})
    idx_value1 = luigi.IntParameter(default=MIGO_ERROR_NUMBER, config_path={"section": "pearson", "name": "idx_value1"})
    idx_value2 = luigi.IntParameter(default=MIGO_ERROR_NUMBER, config_path={"section": "pearson", "name": "idx_value2"})

    def remove_files(self):
        if not self.keep_temp:
            return [self.dest]
        else:
            return []

    def requires(self):
        return CleanerHDFS(self.src)

    def output(self):
        return luigi.hdfs.HdfsTarget(self.dest)

    def mapper(self, line):
        infos = line.split(MIGO_SEPARATOR_LEVEL1)
        key = infos[self.idx_key-1]
        value1 = float(infos[self.idx_value1-1])
        value2 = float(infos[self.idx_value2-1])

        if float(value1) != MIGO_ERROR_NUMBER and float(value2) != MIGO_ERROR_NUMBER:
            yield key, MIGO_TMP_SEPARATOR.join([str(value1), str(value2), str(float(value1)**2), str(float(value2)**2), str(float(value1)*float(value2))])

    def reducer(self, key, values):
        pass

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        def output(key, x, y, x2, y2, xy, n):
            if key != None:
                pearson_cc = (xy - x*y/n) / math.sqrt((x2 - x**2/n)*(y2 - y**2/n))
                print >> stdout, "{}\t{}".format(key, pearson_cc)

        pre_key, sum_x, sum_y, sum_x2, sum_y2, sum_xy, total = None, 0, 0, 0, 0, 0, 0
        for line in stdin:
            key, infos = re.split("\s", line[:-1].replace("'", ""))
            x, y, x2, y2, xy = re.split(MIGO_TMP_SEPARATOR, infos)
            if pre_key != None and pre_key != key:
                ouptut(pre_key, sum_x, sum_y, sum_x2, sum_y2, sum_xy, total)
                sum_x, sum_y, sum_x2, sum_y2, sum_xy, total = 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

            sum_x += float(x)
            sum_y += float(y)
            sum_x2 += float(x2)
            sum_y2 += float(y2)
            sum_xy += float(xy)
            total += 1

            pre_key = key

        output(pre_key, sum_x, sum_y, sum_x2, sum_y2, sum_xy, total)

class MeanStd(MigoLuigiHdfs):
    idx_key = luigi.Parameter(default="", config_path={"section": "sum", "name": "idx-key"})
    idx_value = luigi.Parameter(default="", config_path={"section": "sum", "name": "idx-value"})

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)
        keys = []
        for idx in self.idx_key.split(","):
            keys.append(infos[int(idx)-1])

        values = []
        for idx in self.idx_value.split(","):
            values.append(infos[int(idx)-1])

        yield MIGO_TMP_SEPARATOR.join(keys), MIGO_TMP_SEPARATOR.join(values)

    def reducer(self):
        pass

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        def output(key, total, sum, x2):
            for idx in range(0, len(total)):
                if total[idx] > 1:
                    mean = sum[idx]/total[idx]
                    var = x2[idx]/(total[idx]-1) - mean**2

                    print >> stdout, "{}\t{}\t{}\t{}".format(key.replace(MIGO_TMP_SEPARATOR, MIGO_SEPARATOR_LEVEL1), total[idx], mean, var**0.5)
                else:
                    print >> stdout, "{}\t{}\t{}\t{}".format(key.replace(MIGO_TMP_SEPARATOR, MIGO_SEPARATOR_LEVEL1), total[idx], MIGO_ERROR_NUMBER, MIGO_ERROR_NUMBER)

        sum, x2, total = [], [], []

        pre_key = None
        for line in self.internal_reader((x[:-1] for x in stdin)):
            key, values = line[0], line[1]
            values = values.split(MIGO_TMP_SEPARATOR) 
           
            if pre_key != None and pre_key != key:
                output(pre_key, total, sum, x2)
                sum, x2, total = [], [], []

            if not sum:
                sum = [0.0 for idx in range(0, len(values))]

            if not x2:
                x2 = [0.0 for idx in range(0, len(values))]

            if not total:
                total = [0 for idx in range(0, len(values))]

            for idx in range(0, len(values)):
                value = float(values[idx])

                if value != MIGO_ERROR_NUMBER:
                    sum[idx] += value
                    x2[idx] += value**2
                    total[idx] += 1

            pre_key = key

        output(pre_key, total, sum, x2)

if __name__ == "__main__":
    luigi.run()
