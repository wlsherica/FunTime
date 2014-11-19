
import sys, getopt
sys.path.append("/data/migo/pandora/lib")

from pandora import *
from pyspark import SparkContext

NUMERIC_TYPE = "N"
CATEGORY_TYPE = "C"

def usage():
    print "Usage..."

if __name__=="__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:d:")
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    dimension = {NUMERIC_TYPE: {}, CATEGORY_TYPE: {}}

    for o, a in opts:
        if o == "-i":
            logFile = a
        elif o == "-d":
            for pair in a.split(","):
                [idx, name, category] = pair.split(":")
                idx = int(idx)
                if category.upper() == CATEGORY_TYPE:
                    #dimension[CATEGORY_TYPE] = {idx: name}
                    dimension.setdefault(CATEGORY_TYPE, {})[idx] = name
                elif category.upper() == NUMERIC_TYPE:
                    dimension.setdefault(NUMERIC_TYPE, {})[idx] = name
                    #dimension[NUMERIC_TYPE] = {idx: name}
                else:
                    assert False, "Wrong Column Type - %s, %s, %s" %(idx, name, category)
        else:
            assert False, "Unhandled Option - %s" %o

    print dimension 
