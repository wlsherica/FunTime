#!/usr/bin/env python

import sys
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD, LogisticRegressionWithLBFGS

def parseLine(line):
    #gender, age, y
    gender, age, y = line.split("\t")
    if gender == "Male":
        gender_code = 0
    else:
        gender_code = 1
    return LabeledPoint(y, [gender_code, age])

if __name__ == "__main__":
    sc = SparkContext(appName="LogisticRegPY")
    info = sc.textFile(sys.argv[1]).map(parseLine)

    #Model01: w/ SGD
    #model = LogisticRegressionWithSGD.train(info, 6)

    #Model02: L-BFGS
    model = LogisticRegressionWithLBFGS.train(info)

    #Evaluating the model on training data
    labelsAndPreds = info.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(info.count())

    print "Training Error: {}".format(str(trainErr))

    print "Final weights: {}".format(str(model.weights))
    print "Final intercept: {}".format(str(model.intercept))

    print labelsAndPreds.take(10)

    #print "Data count is {}.".format(counts)
    
    sc.stop()
