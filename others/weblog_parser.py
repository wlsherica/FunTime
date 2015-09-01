#!/usr/bin/env python
#authors: Erica Li
#objective: web log parser
#date: 2015-09-01

inputFile = "/home/erica/grid-access_log" #"/data/grid-access_log"
outputFile = "/home/erica/access_log_10.txt"

import re
import datetime
from datetime import datetime
from user_agents import parse

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

if __name__=="__main__":

    LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+) \"(.+?)\" \"(.+?)\"'

    try:
        with open(inputFile) as file:
            data = file.readlines()
    except IOError as e:
        print "Unable to find Input File"

    start_time = datetime.now()

    #save as files
    with open(outputFile, 'w') as outf:
        for item in data:
            match = re.search(LOG_PATTERN, item)

            host = match.group(1)
            client_identd = match.group(2)
            user_id = match.group(3)
            stime = match.group(4)
            method = match.group(5)
            endpoint = match.group(6)
            protocol = match.group(7)
            response_code = match.group(8)

            last_page = match.group(10)
            user_agent = match.group(11)

            #datetime parser
            #date_time = datetime(int(stime[7:11]), month_map[stime[3:6]], int(stime[0:2]), int(stime[12:14]), int(stime[15:17]), int(stime[18:20]))

            #agent info
            agent_info = parse(user_agent)
            agent = agent_info.device.family
            browser = agent_info.browser.family
            os = agent_info.os.family

            result = [host, client_identd, user_id, stime, method, endpoint, protocol, response_code, last_page, agent, browser, os]
            outf.writelines("%s\n" % ",".join(result))

    outf.close()

    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))

