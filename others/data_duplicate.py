#!/usr/bin/env python
#Objective: Data duplication from mongo API, retrieve store info and generate combination for data
#Author: Erica L Li
#Created Date: 2015.01.05

import sys
sys.path.append("/data/migo/athena/lib")

from athena_variable import *
from athena_luigi import *

import json
import urllib2
from itertools import combinations
import luigi, luigi.hdfs, luigi.hadoop

class DataDuplicate(MigoLuigiHdfs):

    def start(self):
        shop_api = urllib2.urlopen("http://pandora-dev.migocorp.com:8000/lemon/starterdiy/shop/q/kgsupermarket/")
        shop_cate = json.loads(shop_api.read())
        if shop_cate["DATA"]["ShopID"] == "kgsupermarket":
            target = shop_cate["DATA"]["Store"]
            self.mydict = {}

        for id, shop in enumerate(target):
            tg_list = [0,2,1,3]
            key = shop["StoreID"].encode("utf-8").split("^")[1]
            #val = shop.values()[0:4]
            val = map(shop.values().__getitem__, tg_list)
            val_comb = self.combineAll(val)
            self.mydict[key] = val_comb

#    def combineAll(self, inputlist):
#        result = []
#        output = sum([map(list, combinations(inputlist, i)) for i in range(len(inputlist) + 1)], [])
#        for i in output:
#            if len(filter(None, i)) > 0:
#                result.append("^".join(j for j in i if j))
#        return result

    def combineAll(self, inputlist):
        result = []
        output = sum([map(list, combinations(inputlist, i)) for i in range(len(inputlist) + 1)], [])
        for i in output:
            if len(filter(None, i)) > 0:
                if len(i) > 1 and len(i) < 5:
                    idx = [inputlist.index(j) for j in i]
                    tmp = 0
                    score = 0
                    for idz, k in enumerate(idx):
                        ng = len(idx)-1
                        if idz == 0:
                            tmp = k
                            continue
                        score += k - tmp
                        if score == ng and idz == ng and idx[0] == 0:
                            result.append("^".join(j for j in i if j))
                        score = 0
                elif len(i) == 1 and inputlist.index("".join(i)) == 0:
                    result.append("".join(i))
        return result

    def mapper(self, line):
        infos = line.strip().split(MIGO_SEPARATOR_LEVEL1)

        if len(infos) == 4:
            shop_id, member_id, ordate, amount = infos[0], infos[1], infos[2], infos[3]
            shop, branch = shop_id.split("^")

            if branch in self.mydict:               
                self.count_success += 1
                yield "{}\t{}\t{}\t{}".format(shop, member_id, ordate, amount),
                yield "{}\t{}\t{}\t{}".format(shop_id, member_id, ordate, amount),

                for i in self.mydict[branch]:
                    yield "{}^{}\t{}\t{}\t{}".format(shop, i.encode("utf-8"), member_id, ordate, amount),
            else:
                self.count_fail += 1
                yield MIGO_STAMP_FOR_ERROR_RECORD, line

if __name__ == "__main__": 
    luigi.run()
