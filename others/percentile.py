#!/usr/bin/env python
#Create percentile from 1 to 99 by key

import re
import sys
sys.path.append("/data/migo/athena/lib")
import math
import datetime

from itertools import groupby
from athena_luigi import *

import luigi, luigi.hadoop, luigi.hdfs

class Percentile(luigi.hadoop.JobTask):
    source = luigi.Parameter()
    use_hadoop = luigi.BooleanParameter()
    destination = luigi.Parameter()

    tmp = None #"/user/tmp/percentile"

    map_key_sepa ="#"
    partitioned = True
    comparator = False
    cnt = 0
    pshop_id = None

    def initialized(self):
        hdfsClient = luigi.hdfs.HdfsClient()
        self.tmp = '/user/tmp/percentile/' + datetime.date.today().strftime("tmp_%Y%m%d")

        if hdfsClient.exists(self.tmp):
            hdfsClient.remove(self.tmp)
        return super(Percentile, self).initialized()

    def requires(self):
        if self.use_hadoop:
            return CleanerHDFS(self.source)
        else:
            return CleanerHDFS(self.source)

    def jobconfs(self):
        jcs = super(luigi.hadoop.JobTask, self).jobconfs()

        if self.reducer == NotImplemented:
            jcs.append('mapred.reduce.tasks=0')
        else:
            jcs.append('mapred.reduce.tasks=%s' % self.n_reduce_tasks)

        if self.partitioned:
            jcs.append("mapreduce.map.output.key.field.separator=%s" % self.map_key_sepa)
            jcs.append("mapreduce.partition.keypartitioner.options=-k1,1")

        if self.comparator:
            jcs.append("mapreduce.map.output.key.field.separator=%s" % self.map_key_sepa)
            jcs.append("mapreduce.partition.keycomparator.options=-k2,2n")
            jcs.append("mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator")
        return jcs

    def output(self):
        if self.use_hadoop:
            return luigi.hdfs.HdfsTarget(self.tmp)
        else:
            return luigi.LocalTarget(self.tmp)

    def mapper(self, line):
        key, value = line.strip().split("\t")
        yield "%s#%020.4f" % (key,float(value)),1 

    def reducer(self, key, values):
        self.cnt += 1
        yield "{}#{}".format(key, self.cnt),

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        pshop_id = None
        mx_cnt = 0

        for key, values in groupby(inputs, key=lambda x: repr(x[0])):

            shop_id, value = key.strip().replace("'","").split("#")                        

            if pshop_id != None and pshop_id != shop_id:
                yield "{}#!#{}".format(pshop_id, mx_cnt), 
                mx_cnt = 0

            for output in reducer(eval(key), (v[1] for v in values)):
                yield output

            pshop_id = shop_id
            mx_cnt += 1

        if pshop_id != None:
            yield "{}#!#{}".format(pshop_id, mx_cnt),
        self._flush_batch_incr_counter()


class Percentile2(luigi.hadoop.JobTask):
    source = luigi.Parameter()
    use_hadoop = luigi.BooleanParameter()
    destination = luigi.Parameter()

    def initialized(self):
        hdfsClient = luigi.hdfs.HdfsClient()
        if hdfsClient.exists(self.destination):
            hdfsClient.remove(self.destination)
        return super(Percentile2, self).initialized()

    def jobconfs(self):
        jcs = super(luigi.hadoop.JobTask, self).jobconfs()

        jcs.append("mapred.reduce.tasks=25")
        jcs.append("mapreduce.map.output.key.field.separator=_")
        jcs.append("mapreduce.partition.keypartitioner.options=-k1,1")

        return jcs

    def requires(self):
        return Percentile(self.source, self.use_hadoop, self.destination)

    def output(self):
        if self.use_hadoop:
            return luigi.hdfs.HdfsTarget(self.destination)
        else:
            return luigi.LocalTarget(self.destination)

    def mapper(self, line):
        #kgsupermarket^C001#00000000000000000001#1
        #kgsupermarket^C001#!#10
        shop_id, value, idx = line.strip().split("#")
        yield "{}_{}".format(shop_id, value), idx 

    def reducer(self):
        pass

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        #kgsupermarket^C001_!    10
        #kgsupermarket^C001_00000000000000000001 1
        self.init_hadoop()
        self.init_reducer() 

        for line in stdin:
            shop_id, val, idx = re.split("[_\t]", line.strip().replace("'", ""))

            if val == '!':
                count = int(idx)
                pct_dict = {}

                for i in range(1,100):
                    pct_idx = int(round(i*(count - 1)/100 + 1))
                    pct_dict[str(pct_idx)] = i

            if idx in pct_dict:
                pct_h = pct_dict[idx]
                print >> stdout, "{}\t{}\t{}".format(shop_id, pct_h, float(val))


if __name__=="__main__":
    luigi.run()

