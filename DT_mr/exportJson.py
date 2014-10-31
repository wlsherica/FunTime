   #!/usr/bin/env python
#write json file

import sys
import os
import getopt 

try:
    import simplrjson as json
except ImportError:
    import json

def getNodeInfo(filepath1, filepath2, level):

    LEVEL = level 

    text_file1 = open(filepath1,'r')
    line1 = text_file1.read().split(':')
    field = line1[0] #e.g. var00
    rules = line1[1] #Sunny,Overcast,Rainy

    text_file2 = open(filepath2,'r')
    line2 = text_file2.read().split('\t')
    igr = line2[1] #e.g. 0.156

    PARENT='root'

    jj = {"name": field,
          "igr": igr,
          "json": {"level": LEVEL, "name":PARENT+'#'+field , "rules": rules, "parent": PARENT}
         }

    myFile = open('result.txt','w')
    myFile.write(json.dumps(jj))
    myFile.close()

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:n:d",)
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    inputFile=None
    inputDesc=None

    level=0

    for o, a in opts:
        if o == '-p':
            level = a
        elif o == '-n':
            inputFile = a
        elif o == '-d':
            inputDesc = a

    if inputFile==None or not os.path.exists(inputFile):
        print sys.stderr, "Not Found %s" %inputFile
    if inputDesc==None or not os.path.exists(inputDesc):
        print sys.stderr, "Not Found %s" %inputDesc

    if level >= 0:
        getNodeInfo(inputDesc, inputFile, level)

