# coding=UTF-8
#for wordcount
import sys, getopt
import itertools
import math

from pyspark import SparkContext

ENCODING = "utf-8"
NUMERIC_TYPE = "N"
CATEGORY_TYPE = "C"

def usage():
    print "Usage..."

def parserLine(line):
    return line.strip().split('\t')

def smin(a, b):
    return min(a, b)

def smax(a, b):
    return max(a, b)

def sqrt2(i, mean):
    return math.pow(float(i)-float(mean), 2)

def smean(sum, count):
    return sum*1.0/count

def add(a, b):
    return float(a)+float(b)

def tupleDivide(y):
    return float(y[0])/y[1] 

def transform(inputlist, desc):
    if desc not in ["COUNT"]:
        for idx, val in inputlist:
            col1, col2 = idx.split("_")
            tmp[name_C[0]][col1][name_C[1]][col2][name_N[0]][desc] = val
    else:
        for idx, val in inputlist.items():
            col1, col2 = idx.split("_")
            tmp[name_C[0]][col1][name_C[1]][col2][name_N[0]][desc] = val
    return tmp

class AutoDict(dict):
    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value

try:
    import jsonlib2 as json
except:
    import json

def jsonOut(jj, path, sort=True, indent=4):
    try:
        jsondata = json.dumps(jj, sort_keys=sort, indent=indent)

        myFile = open(path,'wb')
        myFile.write(jsondata)
        myFile.close()
    except:
        print >> sys.stderr, 'ERROR writing JSON file', myFile

if __name__=="__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:")
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    for o, a in opts:
        if o == "-i":
            logFile = a
        elif o == "-d":
            for pair in a.split(","):
                [idx, name, category] = pair.split(":")
                idx = int(idx)
                if category.upper() == CATEGORY_TYPE:
                    dimension.setdefault(CATEGORY_TYPE, {})[idx] = name
                elif category.upper() == NUMERIC_TYPE:
                    dimension.setdefault(NUMERIC_TYPE, {})[idx] = name
                else:
                    assert False, "Invalid Column Type - %s, %s, %s" %(idx, name, category)
        else:
            assert False, "Unhandled Options - %s" %o

    sc = SparkContext(appName="3D dimension")
    sgRDD = sc.textFile(logFile)

    getVar = lambda searchList, ind: tuple([searchList[i].encode(ENCODING) for i in ind])

    #info = ["MIN","MAX","SUM","MEAN","SD","COUNT"]

    list_C = dimension['C'].keys()
    list_N = dimension['N'].keys()
    name_C = dimension['C'].values()
    name_N = dimension['N'].values()

    ids = "_".join(str(x) for x in list_C+list_N)
    names = "_".join(name_C+name_N)
    typeColumn = "CCN"

    outputFile = "/home/erica_li/proj/spark_1D/pandora/modules/luigi_1d/bin/tmp/%s_%s_%s.json" %(names, ids, typeColumn)
    pool = {"CATEGORY": typeColumn, "DATA": {}}

    raw = sgRDD.map(lambda x: getVar(parserLine(x),list_C+list_N)).cache()

    raw_cnt = raw.map(lambda (x,y,z): (x+"_"+y, 1)).countByKey()
    raw_sum = raw.map(lambda (x,y,z): (x+"_"+y, z)).reduceByKey(add)
    
    raw_min = raw.map(lambda (x,y,z): (x+"_"+y, z)).reduceByKey(smin)
    raw_max = raw.map(lambda (x,y,z): (x+"_"+y, z)).reduceByKey(smax)

    raw_cntRDD = sc.parallelize(raw_cnt.items(),3)
    raw_mean = raw_sum.join(raw_cntRDD).map(lambda (x, y): (x, tupleDivide(y)))

    raw_sd = raw.map(lambda (x,y,z): (x+"_"+y, z)).join(raw_mean)
    sd0 = raw_sd.map(lambda (x, y): (x, sqrt2(y[0], y[1]))).reduceByKey(add)
    sd = sd0.join(raw_cntRDD).map(lambda (x, y): (x, (tupleDivide(y)**0.5)))

    tmp = AutoDict()

    transform(raw_min.collect(), "MIN")
    transform(raw_max.collect(), "MAX")
    transform(raw_sum.collect(), "SUM")
    transform(raw_mean.collect(), "MEAN")
    transform(sd.collect(), "SD")
    transform(raw_cnt, "COUNT")

    pool["DATA"] = tmp

    jsonOut(pool, outputFile)

    sc.stop()
