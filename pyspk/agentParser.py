# coding=UTF-8
# objectives: agent string parser function
# author: Erica L Li
# created date: 2015-08
# usage: ./bin/spark-submit --executor-memory 10g --driver-memory 10g agentParser.py
# spark-version: 1.3

from pyspark import SparkContext
from user_agents import parse
from pyspark.sql import HiveContext, Row

output_file = "file:///root/tmp/data_3.json"

def deviceDetect(string):
    user_agt = parse(string)
    if user_agt.is_mobile:
        device = "Mobile"
    elif user_agt.is_tablet:
        device = "Tablet"
    elif user_agt.is_touch_capable:
        device = "TouchCapable"
    elif user_agt.is_pc:
        device = "pc"
    elif user_agt.is_bot:
        device = "bot"
    else:
        device = "Others"
    return device

if __name__=="__main__":

    sc = SparkContext(appName="AgentParser")
    hiveCtx = HiveContext(sc)

    #udf
    hiveCtx.registerFunction("getSDevice", lambda x: deviceDetect(x))
    hiveCtx.registerFunction("getBrowser", lambda x: parse(x).browser.family)
    hiveCtx.registerFunction("getOS", lambda x: parse(x).os.family)

    agent = hiveCtx.sql("select eruid, time, getSDevice(agent) as device, getBrowser(agent) as browser, getOS(agent) as OS from ec.ec_testing_exportdata_tbl")

    #save
    agent.save(path=output_file, source="json", mode="append")  

    #close
    sc.stop()
