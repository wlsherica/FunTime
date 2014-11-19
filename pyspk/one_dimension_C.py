#!/usr/bin/env python

import sys

MIGO_HAOOOP_CLUSTER_URL = "hdfs://cdh4-n1.migosoft.com"
MIGO_HADOOP_CLUSTER_URL = "local"
MIGO_MEMORY = "3g"

import math
class PandoraMath(object):
    def __init__(self):
        pass

    @staticmethod
    def min(a,b):
       return min(a, b)

    @staticmethod
    def max(a,b):
       return max(a, b)

    @staticmethod
    def sqrt2(i, mean):
        return math.pow(float(i)-float(mean), 2)

    @staticmethod
    def mean(sum, count):
        return sum*1.0 / count

    @staticmethod
    def add(a, b):
        return float(a) + float(b)

import itertools
from pyspark import SparkContext
from pyspark import SparkConf

class PandoraSCLoader(object):
    def __init__(self, scPath):
        self.scPath = scPath

        theconf = SparkConf().set("spark.executor.memory", MIGO_MEMORY).setAppName("PandoraSCLoader For Dimension Analysis").setMaster(MIGO_HADOOP_CLUSTER_URL)
        self.sc = SparkContext(conf = theconf)
        self.data = self.sc.textFile(self.scPath)
        #self.data.cache()

    def getCombinations(self, keys, num=2):
        return list(itertools.conbimations(keys, num))

    def close(self):
        self.sc.stop()

try:
    import jsonlib2 as json
except:
    import json

class PandoraExporter(object):
    @staticmethod
    def json(jj, path, sort=True, indent=4):
        try:
            jsondata = json.dumps(jj, sort_keys=sort, indent=indent)

            myFile = open(path,'wb')
            myFile.write(jsondata)
            myFile.close()
        except:
            print >> sys.stderr, 'ERROR writing JSON file', myFile
