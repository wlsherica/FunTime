#!/usr/bin/env python 

from datetime import date, timedelta as td

order_date = datetime.strptime(i, "%Y-%m-%dT00:00:00")
caldate=datetime.strptime("20150202", "%Y%m%d")

d1 = date(2014, 8, 25)
d2 = date(2014, 12, 31)

delta = d2 - d1

for i in range(delta.days + 1):
    print d1 + td(days=i)



get_dt = ""
for i in mydate:
    getdate = datetime.datetime.strptime(i, "%Y-%m-%dT00:00:00")
    if get_dt != "" and get_dt != getdate:
        diff = getdate - get_dt
        for j in range(diff.days+1):
            print get_dt + td(days=j)
    get_dt = getdate
print i

