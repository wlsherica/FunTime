#!/usr/bin/env python
import sys
import os
import getopt 

def getNodeInfo(filepath1, filepath2, level):

    text_file1 = open(filepath1,'r')
    line1 = text_file1.read().split(':')
    field = line1[0] #e.g. var00
    subgrp = line1[1].strip('\t\n').split(',') #Sunny,Overcast,Rainy

    text_file2 = open(filepath2,'r')
    line2 = text_file2.read().split('\t')
    igr = line2[1] #e.g. 0.156
    
    if float(igr) != 1 and float(igr) > 0:
        for i in subgrp:
            print ("%s,%s,%d") % (field, i, int(level))
    

def getNextNode(filepath):

    hasNextLayer = 0 

    text_file = open(filepath,'r')
    line2 = text_file.read().split('\t')
    igr = line2[1] #e.g. 0.156    

    if igr != 1 and igr > 0:
        print ("%s#") %s (line2[0])

        hasNextLayer += 1
    
    if hasNextLayer == 0:
        sys.exit(3)

if __name__=="__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "v:f:d:n",)
    except getopt.GetoptError:
        print str(err)
        sys.exit(2) #terminated with unusual

    inputFile=None
    inputDesc=None

    level = 0 
    nextLayer=False

    for o, a in opts:
        if o == "-v":
            level = a 
        elif o == "-f":
            inputFile = a
        elif o == "-d":
            inputDesc = a 
        elif o == "-n":
            nextLayer = True

    #print level, inputFile, inputDesc

    if inputFile == None or not os.path.exists(inputFile):
        print " >>> Not found : %s" %inputFile
        sys.exit(1)
    if inputDesc == None or not os.path.exists(inputDesc):
        print " >>> Not found : %s" %inputDesc
    if level >= 0:
        getNodeInfo(inputDesc, inputFile, level)
    #if level >0 :
        #getNextNode(inputFile)

