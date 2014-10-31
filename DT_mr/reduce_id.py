#!/usr/bin/env python
# objective: pick target data for next tree node calculation
import sys

'''
key	var00;Sunny
key	allcnt
key	var01;Hot	
key	var00;Overcast
	
var00 0.156
'''

if __name__ == "__main__":
    prefix = sys.argv[1].strip().split('\t')
    bag = []
    if 'var' in prefix[0]:
        for line in sys.stdin:
            detail = line.strip().split('\t')
            subGrp = detail[1].split(';')

            if prefix[0] == subGrp[0]:
                bag.append(subGrp[1])

        print "%s:%s" % (prefix[0], ",".join(bag)) #"%s:%s:%s" % (prefix[0], ",".join(bag,prefix[1]))
                #print "%s\t%s" % (detail[0], detail[1])
    #else:
        #for line in sys.stdin:
            #print "%s,%s" %(prefix, line.strip())
