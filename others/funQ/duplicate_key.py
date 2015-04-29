#!/usr/bin/env python
#wlsherica
#2015-04-29
#"cat dog monkey cat"->"cat"

if __name__ == "__main__":

    line = "cat dog monkey cat"
    mydict = {}

    for i in line.split(" "):
        if i in mydict:
            num = mydict[i]
            mydict[i] = num+1
        else:
            mydict[i] = 1

    print "".join([k for k, v in mydict.items() if v > 1])
