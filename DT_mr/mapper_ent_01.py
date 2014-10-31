#! /usr/bin/env python

import sys

'''
var01;tcp;ftp_write.	8
var01;tcp;multihop.	7
var01;tcp;rootkit.	7
var01;tcp;warezclient.	1020
var01;icmp;satan.	37
var01;tcp;back.	2203
var01;tcp;buffer_overflow.	30
var01;tcp;portsweep.	10407
var01;icmp;nmap.	1032
var01;tcp;phf.	4
'''

for line in sys.stdin:
  line = line.strip()
  words = line.split('\t')
  key = words[0]
  key1, key2, key3 = key.split(';')
  value = words[1]
  #value =words[1]  #words[41]  
  #for word in words:
  print "%s\t%s" % (key1+'_top'+';'+key2, key3+';'+value)
