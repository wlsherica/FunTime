#!/usr/bin/pyspark

import re
import sys
import itertools

from operator import add
from pyspark import SparkContext

try:
    import simplrjson as json
except ImportError:
    import json

def json_put(jj):
    myFile=open('/home/training/erica/pyspk/F2D.json','w')
    try:
        jsondata=json.dumps(jj, sort_keys=True, indent=4)
        myFile.write(jsondata)
        myFile.close()
    except:
        print 'ERROR writing JSON file', myFile
        pass

def parserLine(line):
    #parse data line into list
    parts=line.replace("'",'').strip().split('\t')
    return parts

def min_fn(a,b):
    if float(a) < float(b):
        return float(a)
    else:
        return float(b)

def max_fn(a,b):
    if float(a) > float(b):
        return float(a)
    else:
        return float(b)

def sqrt2(i,mean):
        return (float(i)-float(mean))**2

class AutoDict(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

if __name__=="__main__":

    #load data from hadoop
    logFile = "hdfs://cdh4-n.migosoft.com/user/training/reallyLight/Guest.dump"
    #initialize spark context
    sc = SparkContext("local", "Dimension Analysis")
    logData = sc.textFile(logFile)
    #.cache()

    col_list={1:'cartType',14:'payMoney'}
    #{1:'cartType',5:'shopId',14:'payMoney',15:'hTimes'}
    #col_comb2=list(itertools.permutations(col_list.keys(),2))
    col_comb2=list(itertools.combinations(col_list.keys(),2))
    #select column:cardType,shopId,payMoney,hTimes

    pool=AutoDict()

    #Two Dimension
    for i,v in col_comb2:
        v1=col_list.get(i) #cartType
        v2=col_list.get(v) #shopID

        raw=logData.map(lambda x:(parserLine(x)[i].encode('utf-8')+','+parserLine(x)[v].encode('utf-8')))

        gcnt=raw.map(lambda x:(x,1)).reduceByKey(add).collect()
    #    tmp=logData.map(lambda x:(title+','+parserLine(x)[i]+','+parserLine(x)[v],1)).reduceByKey(add)

        sum1=logData.map(lambda x:(parserLine(x)[i].encode('utf-8'),parserLine(x)[v].encode('utf-8'))).reduceByKey(lambda x,y: float(x)+float(y))

        for title, cnt in gcnt:
            t=title.split(',')
            pool[v1][t[0]][v2][t[1]]['COUNT']=cnt
            #print 'NOTE------->', title, cnt            
            #dict((x,y) for x,y in gcnt)

    json_put(pool)
    sc.stop()
