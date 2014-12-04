#! /usr/bin/env python
#coding=utf-8

# author: Erica L Li
# objective: automatically generate many folders or directories

import pyodbc
import random
import os
import re
from datetime import datetime

connStr = 'DRIVER={ODBC Driver 11 for SQL Server};SERVER=s7bidb08;DATABASE=SEO;UID=sqlreadonly;PWD=Read4all'
conn = pyodbc.connect(connStr);cur = conn.cursor();

cur.execute("select LTRIM(RTRIM(IM_subcategory_Desc)), value from DM_SEO_Forecast_PyBase (nolock) ")

# to list : totla 2225
# keep letters and numbers
pattern = '[a-zA-Z0-9]'

test = {}
for it in cur.fetchall():
    str_new=''.join(re.findall(pattern,it[0]))
    if str_new in test:
        test[str_new].append(it[1])
    else:
        test[str_new] = [it[1]]
#IMctlg =list()
#for it in cur.fetchall():
#  IMctlg.append(it[0])

print 'Let us get IM suctegoris: ', len(test)
print 'Start to generate: ' ,datetime.now()

ROOT = r'/home/dmadm/dm_python/ALL_IMctlg/'

for i, ctlg in enumerate(test):
    sw = test[ctlg]
    newpath = os.path.join(ROOT, ctlg)
    bign = str(i)
    if not os.path.exists(newpath):
        os.makedirs(newpath)

for i , line in enumerate(sw):
    flown = bign + '000' + str(i)
    newfile= os.path.join(newpath, flown)
    f = open(newfile,'w')
    line2 = re.sub('[^A-Za-z0-9]+', ' ', line)
    f.write(line2)
    f.close()

#for i, ctlg in enumerate(IMctlg):
#  newpath = os.path.join(ROOT, ctlg)
#  if not os.path.exists(newpath):
#      os.makedirs(newpath)

print 'Finished' , datetime.now()
