#! /usr/bin/env python

import sys

'''
var00_top	0.693536138896
var01_top	0.911063393011
var02_top	0.788450457308
var03_top	0.892158928262

outcome	0.94028595867
var00	1.57740628285
var01	1.55665670746
var02	1.0
var03	0.985228136034
'''

if __name__ == "__main__":
 
    inp_option = float(sys.argv[1])

    #float(sys.argv[1].strip().split('\t')[1])
    ent_all = inp_option 
    top = 0.0

    for line in sys.stdin:
        detail = line.strip().split('\t')
        head = detail[0].split('_')

        if 'outcome' in detail:
            ent_all = float(detail[1])

        else:
            if 'top' in head:
                top = ent_all - float(detail[1])
                print ('%s\t%s') % (head[0], '1;'+str(top))
            else:
                print ('%s\t%s') % (head[0], '2;'+detail[1]) 
