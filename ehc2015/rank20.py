#!/usr/bin/env python
import os
import operator
from pprint import pprint

if __name__ == "__main__" :
    mydict={}
    for line in open('/Users/etu/Desktop/FunTime/EHC/EHC_plist.log'):
        info = line.rstrip('\n').split('=')[1].split(',') 
        if len(info) > 2:
            for i in range(0, len(info), 3):
                item, qt, amt = info[i:i+3]
                val = int(qt)*int(amt)
                if item not in mydict:
                    mydict[item] = val
                else:
                    ori_amt = mydict[item]
                    mydict[item] = ori_amt + val

    sorted_x=sorted(mydict.items(), key=lambda x: (-x[1], x[0]))

    for k, v in sorted_x[:21]:
        print k, v
    #print "Here are top 20 item_key and sales_amount: {}".format(sorted_x[:21])
