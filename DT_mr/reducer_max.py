#!/usr/bin/env python

import sys

'''
root	var00	0.156427562421
root	var01	0.0187726462225
root	var02	0.151835501362
root	var03	0.0488486155113
'''

if __name__ == "__main__":

    ent_max = 0
    pkey = "" 

    for line in sys.stdin:
        detail = line.strip().split("\t")
        key = detail[1]
        val = detail[2]

        if val > ent_max:
            ent_max = val   
            pkey = key

    print ("%s\t%s") % (pkey, ent_max)  
