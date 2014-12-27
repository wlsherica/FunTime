#!/bin/sh

if $(hadoop fs -test -d /user/athena/erica_li) ; then
    echo "File exists"
else
    echo "File does not exists"
fi

hadoop fs -test -d /user/athena/erica_li
TestDir=$?
echo "hello world, ${TestDir}"
