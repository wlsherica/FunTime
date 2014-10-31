#! /usr/bin/env python
import sys

'''
var01;udp	nmap.;250
var01;udp	teardrop.;979
var01;udp	satan.;1708
var01;udp	normal.;191348
var01;udp	rootkit.;3
'''

last_user, value = None, []
u_sum, f_sum = 0, 0  
value2 = []

if __name__ == "__main__":
    for line in sys.stdin:
        #(multi_key, yn_cnt) = line.strip().split('\t')
        infos = line.strip().split('\t')
        multi_key = infos[0]
        yn_cnt = infos[1]

        label, cnt = yn_cnt.split(';')

        bigid = multi_key #id+';'+group
        item = label+';'+cnt

        if last_user != bigid and last_user != None:
            #value = ' '.join(item)
            print "%s\t%s|%s" % (last_user, u_sum, ';'.join(value))
    
            value = []#(user_count, user_sum, value) = (0,0,[])
            u_sum = 0

        last_user = bigid
        value.append(item)
        #f_sum += 1
        u_sum += int(cnt)
        value2 = ';'.join(value)

    print "%s\t%s|%s" % (last_user, u_sum, value2)
