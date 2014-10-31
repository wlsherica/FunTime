#!/bin/sh

outputFile=title.csv

if [ ! -f ${outputFile} ]
then
    echo "The output file ${outputFile} does not exist"
else
    rm ${outputFile}
fi

scrapy crawl shrimp -o ${outputFile} -t csv
