#!/usr/bin/pyspark

import re
import sys
import itertools

from operator import add
from pyspark import SparkContext

import jsonlib2 as json
#try:
#    import simplrjson as json
#except ImportError:
#    import json

def json_put(jj):
    myFile=open('/home/training/erica/pyspk/F1D.json','w')
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

def doRe(data):
    j=0
    mylist=[]
    for i in data:
        mylist[j]=i.encode('utf-8').lower()
        j+=1
    return mylist

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

if __name__=="__main__":

    #load data from hadoop
    logFile = "hdfs://cdh4-n.migosoft.com/user/training/reallyLight/Guest.dump"
    #initialize spark context
    sc = SparkContext("local", "Dimension Analysis")
    logData = sc.textFile(logFile)
    #.cache()

    col_list={1:'cartType',14:'payMoney'}
    #{1:'cartType',5:'shopId',14:'payMoney',15:'hTimes'}
    col_comb2=list(itertools.combinations(col_list.keys(),2))
    #select column:cardType,shopId,payMoney,hTimes

    mypath ='/home/training/erica/pyspk/result'
    pool={}

    #One Dimension
    for i in col_list.keys():
        title=col_list.get(i)

        raw=logData.map(lambda x:(parserLine(x)[i].encode('utf-8')))
        raw_fit=raw.filter(lambda x: re.match("^\d+.+?$",x)!=None)
        crit=raw_fit.take(1)

        cnt=raw.count()
        gcnt=raw.map(lambda x:(x,1)).reduceByKey(add).collect()

        if crit:
            sum1=raw_fit.reduce(lambda x,y: float(x)+float(y))
            min=raw.reduce(min_fn)
            max=raw.reduce(max_fn)
            mean=sum1/cnt
            SD0=raw.map(lambda x:sqrt2(x,mean)).reduce(add)
            SD=(SD0/cnt)**0.5

        else:
            sum1,max,min,SD,mean=[ [] for dummy in range(5)]

        pool_2={}
        pool_2['SUM']=sum1
        pool_2['MAX']=max
        pool_2['MIN']=min
        pool_2['SD']=SD
        pool_2['COUNT']=cnt
        pool_2['MEAN']=mean
        pool_2['CATEGORY']=gcnt
        pool[title]=pool_2

    json_put(pool)

    sc.stop()
