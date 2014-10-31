#!/usr/bin/env python 

import sys 
import math 

'''
allcnt	14
outcome;No	5
outcome;Yes	9
var00;Overcast	4
var00;Rainy	5
var00;Sunny	5
'''

if __name__ == "__main__":

    inp_option = float(sys.argv[1].strip().split('\t')[1])
    key  = ""
    all_cnt = inp_option 
    entropy = 0 

    for line in sys.stdin:
        detail = line.strip().split('\t')
        
        if "allcnt" in detail:
            all_cnt = float(detail[1])
        else:
            group = detail[0].split(';')
            key = group[0]
            top = float(detail[1])
            prob = top / all_cnt
            entropy = -prob*math.log(prob,2)

            print ('%s\t%s') % (key, entropy)
