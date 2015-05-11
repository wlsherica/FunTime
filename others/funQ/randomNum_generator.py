#!/usr/bin/env python
#2015-05-11
#Create a random number generator Xn+1 = (A*Xn+B)%M, A, B, M, Xn will be given
#Output: print random number from K to K+4
#Usage: python yourcode.py "9 7 6 1 3"

import sys

def RandomGen5(line):
    A, B, Xi, K, M = map(int, line)
    return (A*Xi+B)%M

if __name__ == "__main__":

    if len(sys.argv) == 2:
        mystr = sys.argv[1]
    else:
        print "Please provide input string. "
        sys.exit("Sorry, goodbye.")

    alist = mystr.split(" ")
    k = int(alist[3])
    int_pre=None

    for i in range(1, k+5):
        if int_pre is None:
            x = alist[2]
            if k == i:
                print x, "\n"
            int_pre = x
        else:
            alist[2] = int_pre
            int_pre = RandomGen5(alist)
            if i >= k and i <= k+4:
                print int_pre, "\n"

