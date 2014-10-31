#!/usr/bin/env python

import sys

'''
var00	0.156427562421
var01	0.0187726462225
var02	0.151835501362
var03	0.0488486155113
'''

if __name__ == "__main__" :

    for line in sys.stdin:
        detail = line.strip().split('\t')
        key = detail[0]
        val = detail[1]
        
        print ("root\t%s\t%s") % (key, val) #("%s\t%s") % (key, val)
