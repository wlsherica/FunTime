#!/usr/bin/env python
#Erica Li
#2015-04-29
#[1, 2, 3, 5, 8, 12, 13, 14, 15]=>[1~3, 5, 8, 12~15]

if __name__ == "__main__":
    line = [1, 2, 3, 5, 8, 12, 13, 15, 14]

    start=""
    prei=""
    mylist=[]

    for i in sorted(line):
        if start != "":
            if i-int(prei) != 1:
                if start != prei:
                    mylist.append("{}~{}".format(start, prei))
                else:
                    mylist.append(str(prei))
                start = i
                prei = i
            else:
                prei = i   
        else:
            start = i
            prei = i

    mylist.append("{}~{}".format(start, prei))
    print mylist 

