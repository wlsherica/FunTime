#!/usr/bin/env python

import sys

'''
var00	1;0.246749819774
var00	2;1.57740628285
var01	1;0.029222565659
var01	2;1.55665670746
var02	1;0.151835501362
var02	2;1.0
var03	1;0.048127030408
var03	2;0.985228136034
'''
if __name__ == "__main__":

    pkey = ""
    all_score = 0.0 

    for line in sys.stdin:
        detail = line.strip().split('\t')
        info = detail[1].split(';')

        key = detail[0]
        score = float(info[1]) 
        
        if pkey != "" and pkey != key :
            print ("%s\t%s") % (pkey, all_score)
            all_score = score
        else:
            if pkey == "":
                all_score = score 
            else:
                if info[0] == '1':
                    if all_score == 0.0:
                        all_score = 0.0
                    else:
                        all_score = score / all_score 
                else :
                    if score == 0.0:
                        all_score = 0.0
                    else:
                        all_score = all_score / score 
        pkey = key 

    print ("%s\t%s") % (pkey, all_score)
