#!/usr/bin/env python

import sys

'''
outcome	0.409776377538
outcome	0.530509581132
var00	0.516387120588
var00	0.530509581132
var00	0.530509581132
'''

if __name__ == "__main__":
    allscore = 0
    pkey = ""

    for line in sys.stdin:
        detail = line.strip().split("\t")

        key = detail[0]
        score = float(detail[1])

        if pkey != "" and pkey != key:
            print ("%s\t%s") % (pkey, allscore)

            allscore = score  
        else: 
            allscore += score 

        pkey = key 

    print ("%s\t%s") % (pkey, allscore)
