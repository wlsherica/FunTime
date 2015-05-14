#!/bin/bash

#92.168.60.9 - - [11/Apr/2015:11:35:31 +0800] "GET /action?;group=ec;cid=ecmall;act=cart;pid=99999;cat=56335%2C563355%2C563355%2C5633567;uid=1234567;ercampid=Home_800x200AD5Tab3Product_AD_5_1_pic;eradurl=DMSetting%2FFinal%2F201502%2FSP_1029275%2Findex.html;eraddsc=800x200-2-213156501.jpg;ptuple=%22%7B%20paypid%3A%5C%221504461%5C%22%2Cqty%3A%5C%221%5C%22%2Cunit_price%3A%5C%22100222%5C%22%2Cpcat%3A%5C%2256335%2C563355%2C563355%2C5633567%5C%22%2Cpmk%3A%5C%22discount_1%5C%22%20%7D%2C%7B%20paypid%3A%5C%221504463%5C%22%2Cqty%3A%5C%223%5C%22%2Cunit_price%3A%5C%22100333%5C%22%2Cpcat%3A%5C%2256335%2C563355%2C563355%2C5633562%5C%22%2Cpmk%3A%5C%22discount_2%5C%22%20%7D%22;lo=1;;ssid=1cx4lq6cjmrle;erUid=4e485a1-7286-fe57-32e3-3583de4ace84; HTTP/1.1" 200 43 "http://etuvm.com/bbb.html" "curl/7.30.0"

sourcefile=/home/etu/tmp_erica/log.dat
outputfile=log.output
datetime=
pid=
ip=

if [ ! -e "${outputfile}" ]; then
    touch "${outputfile}"
else
    truncate -s 0 "${outputfile}"
fi

while read line;
do
    if [[ "$line" = *act=cart* ]]; then
    pid=$(echo $line | awk -F '"' '{print $2}' | awk -F ';' '{print $5}' | cut -c5- )
    ip=$(echo $line | awk '{print $1}')
    datetime=$(echo $line | awk '{print $4}' | awk -F '[' '{print $2}' | awk -F ':' '{print $1}' )
    echo "$ip" "$pid" "$datetime"
    #printf "$ip\t$datetime\t$pid \n" >> ${outputfile}
    fi
done < ${sourcefile}
