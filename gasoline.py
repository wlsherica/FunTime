#!/usr/bin/env python
# calculate the best L for gasoline
import math
import decimal
from decimal import *

def gas(unitprice):
    i = 1.50
    result = []
    while i <= 3.20 :
        p1 = i*(float(unitprice)-0.8)
        p_temp = format(p1, '.2f')
        q  = round((decimal.Decimal(str(p_temp))),1)
        q_str=str(q)
        q2 = round((decimal.Decimal(str(q))),0)
        q3 = math.floor(p1)
        if q2==q3 and q_str[3]=='4':
            z = ("%.2f %.2f %.1f" % (i, p1, q))
            result.append(z)
      #print ("%.2f %.2f %.1f %.0f %s" % (i, p1, q, q2, q3))
        i += 0.01
    return result

def main():
#while True:
    print u'''Hello, please type the unit price of 95 unlead
'''
    strChoice = raw_input('Select :')
    if strChoice.replace('.','',1).isdigit():
        z = gas(strChoice)
        for i in z:
            print i
    else:
        print 'Sorry, it is not valid digit'

if __name__ == '__main__':
  main()

