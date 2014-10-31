#! /usr/bin/env python
import sys
import re 
import math 

'''
var00;Overcast	4|Yes;4
var00;Rainy	5|No;2;Yes;3
var00;Sunny	5|No;3;Yes;2
'''

all_cnt = float(sys.argv[1]) #count of data

for line in sys.stdin:
  (k1, k2) = line.strip().split('\t')
  k1_1, k1_2 = k1.split(';')
  (sum_cnt, numerator) = k2.strip().split('|')

  #n = numerator.count(';') 
  numerator2 = re.sub(r'[^\d-]+', ' ', numerator)
  num3 = numerator2.strip().split(' ')
  sum_cnt_0 = float(sum_cnt)
  score = 0 

  for i in num3:
    prob = float(i)/sum_cnt_0
    ent_0 = -prob*math.log(prob,2)
    score += ent_0
  #weighted 
  ent_wt = score*(sum_cnt_0/all_cnt)
  print "%s\t%s" % (k1_1, ent_wt)

