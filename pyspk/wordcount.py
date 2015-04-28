#!/usr/bin/env python

import sys
from operator import add
from pyspark import SparkContext

if __name__ == "__main__":

    sc = SparkContext(appName="WordCountPy")
    textFile = sc.textFile("file:///Users/data/news.txt")

    counts = textFile.flatMap(lambda x: x.split(' ')) \
                     .map(lambda x: (x, 1)) \
                     .reduceByKey(add)

    output = counts.collect()
    output_srt = sorted(output, key=lambda x:x[1], reverse=True)

    for (word, count) in output_srt:
        print("%s: %i" % (word, count))

