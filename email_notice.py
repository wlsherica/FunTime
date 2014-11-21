#!/usr/bin/env python

import sys
import json
import os.path
import smtplib

if __name__ == "__main__":
  
    #js_file ='/home/training/erica/japan/jpHotel/hotel.json'
    #try:
    #    os.path.isfile(js_file)
    #except IOError:
    #    print 'Oh no..File is missing'
    #    sys.exit(0)

    #json_data=open(js_file)
    #data = json.load(json_data)
    #max_month = data[-1]

    #result = ', '.join(['{}_{}'.format(k,v) for k,v in max_month.iteritems()])

    sender='erica_li@migocorp.com'
    receivers = ['erica_li@migocorp.com']
    #message = 'The contents of %s' % (result)

    message = '''From: From Person <from@fromdomain.com>
To: To Person <to@todomain.com>
Subject: Crontab note for backup on elephant03

This is a note from crontab. %s
''' % ("Erica L Li")

    try:
       smtpObj = smtplib.SMTP('localhost')
       smtpObj.sendmail(sender, receivers, message)         
       print "Successfully sent email"
    except SMTPException:
       print "Error: unable to send email"



