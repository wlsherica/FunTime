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

    logFile = "hdfs://cdh4-n.migosoft.com/user/erica_li/spktest.dat"

    #spark = SparkContext(appName="simple dimension")
    #sgRDD = spark.textFile(sys.argv[1])
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

    sc = SparkContext(appName="2D dimension")
    sgRDD = sc.textFile(logFile)

    getVar = lambda searchList, ind: [searchList[i] for i in ind]

    id_list = dimension['C'].keys()
    id_name = dimension['C'].values()
    ids = "_".join(str(x) for x in id_list)
    names = "_".join(id_name)
    typeColumn = "CC"
 
    pool = {"CATEGORY": typeColumn, "DATA": {}}
    gcnt = AutoDict()

    outputFile = "/home/erica_li/proj/spark_1D/pandora/modules/luigi_1d/bin/tmp/%s_%s_%s.json" %(names, ids, typeColumn)

    #if 6 not in id_list:
    raw = sgRDD.map(lambda x:(("_".join(getVar(parserLine(x),id_list)).encode(ENCODING))))
    cdict = raw.map(lambda word:(word, 1)).countByKey()

    for columns, val in cdict.items():
        col1, col2 = columns.split("_")
        gcnt[id_name[0]][col1][id_name[1]][col2]['COUNT'] = val
    #else:
    #    tag = {0: 'NES', 1: 'L', 2: 'R', 3: 'F', 4: 'M'}
    #    for i in range(0,5):
    #        raw = sgRDD.map(lambda x:(parserLine(x)[id_list[0]].encode(ENCODING)+'_'+parserLine(x)[6].split(",")[i].encode(ENCODING)))
    #        cdict = raw.map(lambda word:(word, 1)).countByKey()
    #        tmp = {}
    #        for columns, val in cdict.items():
    #            col1, col2 = columns.split("_")
    #            gcnt[id_name[0]][col1][id_name[1]][tag[i]][col2]['COUNT'] = val

    pool["DATA"] = gcnt
    jsonOut(pool, outputFile)

    sc.stop()

