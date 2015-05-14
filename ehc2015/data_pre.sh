#!/bin/bash

sourcePath=/Users/etu/Desktop/FunTime/EHC
zcat < ${sourcePath}/EHC_1st.tar.gz | grep -a act=order | awk -F ';' '{print $4}' > Test_plist.log

python rank20.py
