#!/bin/sh

python --version

outputFile=$1
outputFile=/home/training/erica/japan/jpHotel/hotel.json
pycode=/home/training/erica/japan/jpHotel/test_mail.py

if [ ! -f ${outputFile} ]
then 
    echo "The output file ${outputFile} does not exist"
else
    rm ${outputFile}
fi

cd /home/training/erica/japan/jpHotel/
PATH=$PATH:/usr/local/bin
export PATH
scrapy crawl HotelSpider -o hotel.json -t json

python ${pycode}
