#!/usr/bin/env python
#Objective: Data duplication from mongo API, retrieve store info and generate combination for data
#Author: Erica L Li
#Created Date: 2015.01.05

import sys

from athena_variable import *
from athena_luigi import *

import json
import urllib2
from itertools import combinations
import luigi, luigi.hdfs, luigi.hadoop

class DataDuplicate(MigoLuigiHdfs):

    def combineAll(self, inputlist):
        result = []
        output = sum([map(list, combinations(inputlist, i)) for i in range(len(inputlist) + 1)], [])
        for i in output:
            if len(i) > 0:
                result.append("^".join(i))
        return result

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)

        self.shop_api = urllib2.urlopen("http://pandora-dev.migocorp.com:8000/lemon/starterdiy/shop/q/kgsupermarket/")
        shop_cate = json.loads(self.shop_api.read())
        target = shop_cate["DATA"]["Store"]
        mydict = {}

        for id, shop in enumerate(target):
            key = shop["StoreID"].encode("utf-8").split("^")[1] 
            val = shop.values()[0:4]
            val_comb = self.combineAll(val)
            mydict[key] = val_comb

        if len(infos) == 4:
            shop_id, member_id, ordate, amount = infos[0], infos[1], infos[2], infos[3]
            shop, branch = shop_id.split("^")

            if branch in mydict:
                
                self.count_success += 1
                yield "{}\t{}\t{}\t{}".format(shop_id, member_id, ordate, amount),

                for i in mydict[branch]:
                    yield "{}^{}\t{}\t{}\t{}".format(shop, i.encode("utf-8"), member_id, ordate, amount),
            else:
                self.count_fail += 1
                yield MIGO_STAMP_FOR_ERROR_RECORD, line

if __name__ == "__main__": 
    luigi.run()
