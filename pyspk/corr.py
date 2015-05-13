#!/usr/bin/env python

import sys
import itertools
from pyspark import SparkContext
from pyspark.mllib.stat import Statistics

#input dataformat: age weight oxygen runtime
#Added pairwise missing value delete function

ENCODING = "utf-8"

def idxRdd(oriRdd, idx1, idx2, whr, flt):
    #idx1 = filter clue, idx2 = map clue, whr=1=in else=not in, flt = filter list
    if whr != 0:
        finalRdd = oriRdd.zipWithIndex()\
                 .filter(lambda x: x[idx1] in flt)\
                 .map(lambda x: x[idx2])
    else:
        finalRdd = oriRdd.zipWithIndex()\
                 .filter(lambda x: x[idx1] not in flt)\
                 .map(lambda x: x[idx2])
    return finalRdd


def parserLine(line):
    infos =  line.strip().split(" ")
    #new_infos = [x if x != "." else "0" for x in infos]
    return infos

if __name__ == "__main__":
    sc = SparkContext(appName="PearsonCorrPY")
    info = sc.textFile(sys.argv[1]).map(parserLine)

    #data schema
    mydict = {0: "Age", 1: "Weight", 2: "Oxygen", 3: "runtime"}
    col_comb =[(2, 3)] #list(itertools.combinations(mydict.keys(),2))

    for k, v in col_comb:
        seriesX = info.map(lambda x:(x[k].encode(ENCODING)))
        seriesY = info.map(lambda x:(x[v].encode(ENCODING)))

        d1=idxRdd(seriesX, 0, 1, 1, ["."])
        d2=idxRdd(seriesY, 0, 1, 1, ["."])

        del_list=d1.union(d2).collect()

        t1=idxRdd(seriesX, 1, 0, 0, del_list)
        t2=idxRdd(seriesY, 1, 0, 0, del_list)

        corr = Statistics.corr(t1, t2, method="pearson")
        print "{} vs. {} correlation coefficient = {}".format(mydict[k], mydict[v], corr)

    sc.stop()
