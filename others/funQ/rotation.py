#!/usr/bin/env python
#2015-05-22
#Did line rotation

#input format
"""
5
abcde
2
2 4 1
1 5 2
"""

N = 5
S = 'abcde'
M = 2
#L1, R1, K1 = map(int, "2 4 1".split(' '))
#L2, R2, K2 = map(int, "1 5 2".split(' '))

def u(mystr, n):
    if n == 0:
        return rotate(mystr)
    return rotate(u(mystr, n-1))

def rotate(mystr):
    return mystr[-1:] + mystr[:-1]

inputList = []

#N = int(raw_input())
#S = raw_input()
#M = int(raw_input())
#inputList.append(raw_input())
inputList=["2 4 1","1 5 2"]
strpre=""

if __name__ == "__main__":
    for i in inputList:
        L, R, K = map(int, i.split(' '))
        if strpre == "":
            strpre = S
        for j in range(K):
            str1=str2=""
            if L > 0:
                str1 = strpre[0:L-1]
            if R < N:
                str2 = strpre[R:]
            if L==0 and R ==N:
                body = u(strpre[0:5], K)
            else:
                body = u(strpre[L-1:R],0)
            strpre = str1+str(body)+str2

    #final answer
    print strpre

