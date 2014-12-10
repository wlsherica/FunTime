# coding=UTF-8
#for wordcount
#usage on standalone master mode:./bin/spark-submit --master spark://cdh4-dn2:7077 --executor-memory 3g --driver-memory 1g /data/tmp/wordcount.py hdfs://cdh4-n.migosoft.com/user/rungchi/card_member/part-00000
#./bin/spark-submit --master spark://cdh4-dn2:7077 --executor-memory 3g --driver-memory 1g /home/erica_li/proj/spark_1D/pandora/modules/luigi_1d/bin/simple.py -d "4:Province:C" -i hdfs://cdh4-n.migosoft.com/user/erica_li/spktest.dat
#/data/spark/spark-1.0.2-bin-hadoop2/bin/spark-submit 1d.py -d "14:money:N"
import sys, getopt
import itertools
import math

sys.path.append("/data/migo/pandora/lib")
#from pandora import *
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

def mean(sum, count):
    return sum*1.0/count

def add(a, b):
    return float(a)+float(b)

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

    logFile = "hdfs://cdh4-n.migosoft.com/user/erica_li/spktest.dat"

    dimension = {NUMERIC_TYPE: {}, CATEGORY_TYPE: {}}

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

    sc = SparkContext(appName="simple dimension")
    sgRDD = sc.textFile(logFile)

    #One Dimension
    for typeColumn, column in dimension.items():
        for idx, name in column.items():
            outputFile = "/home/erica_li/proj/spark_1D/pandora/modules/luigi_1d/bin/tmp/%s_%d_%s.json" %(name, idx, typeColumn)

            raw = sgRDD.map(lambda x:(parserLine(x)[idx].encode(ENCODING))).cache()
            cnt = raw.count()
            gcnt = {}

            pool = {"CATEGORY": typeColumn, "IDX": idx, "NAME": name, "DATA": {}}

            if typeColumn == NUMERIC_TYPE:
                sum1 = raw.reduce(add)
                minV = raw.reduce(smin)
                maxV = raw.reduce(smax)
                meanV = mean(sum1, cnt)

                SD0 = raw.map(lambda x: sqrt2(x, meanV)).reduce(add)
                sd = (SD0/cnt)**0.5

                pool["DATA"] = {"SUM": sum1, "MAX": maxV, "MIN": minV, "SD": sd, "COUNT": cnt, "MEAN": meanV}

            elif typeColumn == CATEGORY_TYPE:
                #if idx != 6:
                    #gcnt = raw.map(lambda word:(word, 1)).reduceByKey(add).collect()
                cdict = raw.map(lambda word:(word, 1)).countByKey()
                for k, v in cdict.items():
                    gcnt[k] = v
                #else:
                #    tag = {0: 'NES', 1: 'L', 2: 'R', 3: 'F', 4: 'M'}
                #    for i in range(0,5):
                #        #tagdict = raw.map(lambda x:x.split(",")[i]).map(lambda word:(word,1)).reduceByKey(add).collect()
                #        tagdict = raw.map(lambda x:x.split(",")[i]).map(lambda word:(word,1)).countByKey()
                #        tmp = {}
                #        for k, v in tagdict.items():
                #            tmp[k] = v
                #        gcnt[tag[i]] = tmp

                pool["DATA"] = gcnt

            jsonOut(pool, outputFile)

    sc.stop()
