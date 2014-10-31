#!/usr/bin/env python
# objective: pick target data for next tree node calculation
import sys 

'''
allcnt	14
outcome;No	5
outcome;Yes	9
var00;Overcast	4

var00 0.156
'''

if __name__ == "__main__":
    for line in sys.stdin:
        detail = line.strip().split('\t')

        print "%s\t%s" % ('key',detail[0])
