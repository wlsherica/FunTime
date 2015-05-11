#!/usr/bin/env python
#Date: 2015-05-11
#Print charactor in descending order of the most freq occurence
#e.g. 'acbacbab' -> a b c

import sys

defstr='dcbadcbadcbadde'

if __name__ == "__main__":

    if len(sys.argv) == 2:
        mystr = sys.argv[1]
    else: 
        mystr = defstr

    mydict = {}

    for i in mystr:
        if i not in mydict:
            mydict[i] =1
        else:
            mydict[i] += 1

    lista = mydict.items()
    tuplea = sorted(lista, key = lambda x:(-x[1], x[0]))

    for v1, v2 in tuplea:
        print v1
