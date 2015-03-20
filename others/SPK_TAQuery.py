#coding=UTF-8

import os, sys, getopt
import shutil 
import itertools
import math

sys.path.append("/data/migo/pandora/lib")
from pandora import *
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row

#usage: #./bin/spark-submit /home/erica_li/proj/migo/athena/modules/spark_ta/bin/ta_query.py -d "kgsupermarket^C001:L7D:20140818:16384:0:0" -i hdfs://cdh4-n.migosoft.com/user/athena/ta
#{
#    “StoreID”:”Able^XXX^XXX"
#    “PeriodType”:”L7D”,
#    “CalDate”:”2015-02-03”,
#    “Value1”:”30”,
#    “Value2”:”45”,
#    “Value3”:”56”,
#    “Member”:["1234","2234","3234","4234"………]
# }

try:
    import jsonlib2 as json
except:
    import json

def jsonOut(jj, path):
    try:
        jsondata = json.dumps(jj)
        myFile = open(path,'wb')
        myFile.write(jsondata)
        myFile.close()
    except:
        print >> sys.stderr, 'ERROR writing JSON file', myFile



if __name__=="__main__":

    #-d 9:Gender:C -i hdfs://cdh4-n.migosoft.com/user/athena/personal_all_20141205/
    #-d kgsupermarket^C001:L7D:20140818:16384:0:0 -i hdfs://cdh4-n.migosoft.com/user/athena/kgsupermarket/ta/

    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:")
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    defaultSrc = "hdfs://cdh4-n.migosoft.com/user/athena/ta/20140818/*"
    rule_dict = {"Store_id":"", "PeriodType":"", "CalDate":"", "Value1":"", "Value2":"", "Value3":""}
    map_dict = {1:"Store_id", 2:"PeriodType", 3:"CalDate", 4:"Value1", 5:"Value2", 6:"Value3"}

    for o, a in opts:
        if o == "-i":
            defaultSrc = a
        elif o == "-d":
            for pair in a.split(","):
                if pair:
                    infos = pair.split(":")
                    Store_id, PeriodType, CalDate, Value1, Value2, Value3 = infos
                    Shop_id = Store_id.split("^")[0]

                    for j in range(0, len(infos)):
                        rule_dict[map_dict[j+1]] = infos[j]
                else:
                    assert False, "Invalid Query Parameter - {}".format(pair)
        else:
            assert False, "Unhandled Options - %s" %o

    sc = SparkContext(appName="TA Query")
    sqlContext = SQLContext(sc)

    #load the target dataset
    targetFile = "hdfs://cdh4-n.migosoft.com/user/athena/ta/{caldate}".format(caldate=CalDate)
    textFile = sc.textFile(targetFile)

    data = textFile.map(lambda l: l.split("\t"))
    ta_raw = data.map(lambda p: Row(Store_id=p[0], CalDate=p[1], PeriodType=p[2], member_id=p[3], Value1=p[4], Value2=p[5], Value3=p[6]))

    schemaTag= sqlContext.inferSchema(ta_raw)
    schemaTag.registerTempTable("ta_raw")

    rules = " and ".join(["{} = '{}'".format(k, v) for k, v in rule_dict.items() if v or v != "NULL"])
    output_path = "_".join(["{}".format(v) for k, v in rule_dict.items() if v or v != "NULL"])

    cmd = "SELECT member_id FROM ta_raw WHERE {} LIMIT 12".format(rules)
    query_info = sqlContext.sql(cmd)

    output = query_info.map(lambda p: p.member_id)
    rule_dict["Member"] = output.collect()

    output_File = "/home/erica_li/tmp/{}.txt".format(output_path)
    if os.path.exists(output_File):
        os.remove(output_File)
        print "remove folder already"
    else:
        pass

    jsonOut(rule_dict, output_File)

    #os.system("scp foo.bar joe@srvr.net:/path/to/foo.bar")  

    sc.stop()
