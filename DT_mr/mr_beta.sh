#!/bin/sh

level=
node=
pythonUtil=screwDriver.py

inputForMR=$1
inputForMR=/user/training/decisionTree/source/training.2.txt
#/user/training/el16_test/training.1.txt
#/user/training/decisionTree/source/${inputForMR}

outputForLocal=$2
outputForLocal=mr
if [ -z ${outputForLocal} ]; then
    echo "Not Found ${outputForLocal}"
    exit 4
fi

if [ ! -e ${outputForLocal} ]; then
    mkdir ${outputForLocal}
fi
outputForMR=/user/training/el16_test/${outputForLocal}

#inputForMR01=/user/training/decisionTree/source/training.1.txt  #/user/training/kddcup/source/kddcup.data.gz 
#inputForMR06=/user/training/el16_test/output/output00*

#outputForMR01=/user/training/el16_test/output/output01
#outputForMR02=/user/training/el16_test/output/output002
#outputForMR03=/user/training/el16_test/output/output03
#outputForMR04=/user/training/el16_test/output/output04
#outputForMR05=/user/training/el16_test/output/output005
#outputForMR06=/user/training/el16_test/output/output06
#outputForMR07=/user/training/el16_test/output/output07

function getmerge(){
	local outputFolder=$1
    local resultFile=$2

    rm -f ${resultFile}
    hadoop fs -getmerge ${outputFolder} ${resultFile}

    ret=$?
    if [ ${ret} -ne 0 ]; then
      echo "Mmmm Failed in hadoop fs -getmerge ${outputFolder} ${resultFile} (ret=${ret})"
      exit 999
    fi
}

function isEmpty(){
    local realFile=$1
    appendFile=empty.txt

    if [ -e "${realFile}" ]; then
        appendFile=${realFile}
    else
        touch empty.txt
    fi

    echo ${appendFile}
}

function mr(){
    local inputFolder=$1
    local outputFolder=$2
    local mapperFile=$3
    local mapper=$4
    local reducerFile=$5
    local reducer=$6

    #mapperFile=$(isEmpty ${mapperFile})
    reducerFile=$(isEmpty ${reducerFile})    

    hadoop fs -test -e ${outputFolder}
    if [ $? -eq 0 ]; then
        hadoop fs -rm -r ${outputFolder}
    fi

    hadoop jar /opt/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.47/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -input "${inputFolder}" \
    -output "${outputFolder}" \
    -file "${mapperFile}" \
    -mapper "${mapper}" \
    -file "${reducerFile}" \
    -reducer "${reducer}"

    ret=$?
    if [ ${ret} -ne 0 ]; then
        echo "Fail(ret=${ret})"
        exit${ret}
    fi
}

function treenode(){

    local level=$1
    local node=$2 

    local nodeFile=$3
    local fileDescription=$4

    local tmpForMR01=$5 #outputForMR01
    local tmpForMR02=$6
    local tmpForMR03=$7
    local tmpForMR04=$8
    local tmpForMR05=$9
    local tmpForMR06=${10} 
    local tmpForMR07=${11}
    
    local tmpInputForMR=${12} #inputForMR01
    local inputForMR06=${13} 
    local tmpForMR08=${14}

    #/user/training/el16_test/output/output01/part*
    #/user/training/decisionTree/source/training.1.txt
    #/user/training/el16_test/output/output002

    mr "${tmpInputForMR}" "${tmpForMR01}" "mapper_ent_all.py" "mapper_ent_all.py ${level} ${node}" "reducer.py" "reducer.py"

    local g=$(hadoop fs -ls ${tmpForMR01} | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep allcnt; done)

    mr "${tmpForMR01}" "${tmpForMR02}" "mapper_ent_all_02.py" "mapper_ent_all_02.py ${g}" "reducer_ent_all_02.py" "reducer_ent_all_02.py"

    mr "${tmpInputForMR}" "${tmpForMR03}" "mapper_ent_00_v2.py" "mapper_ent_00_v2.py ${level} ${node}" "reducer.py" "reducer.py"

    mr "${tmpForMR03}" "${tmpForMR04}" "mapper_ent_01.py" "mapper_ent_01.py" "reducer_ent_01.py" "reducer_ent_01.py"
    local f=$(hadoop fs -ls ${tmpForMR01} | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep allcnt | cut  -f2; done)

    mr "${tmpForMR04}" "${tmpForMR05}" "mapper_ent_02.py" "mapper_ent_02.py ${f}" "reducer_ent_all_02.py" "reducer_ent_all_02.py"

    local j=$(hadoop fs -ls ${tmpForMR02} | awk '{print $8}' | while read f; do hadoop fs -cat $f | grep outcome | cut -f2; done)
    mr "${inputForMR06}" "${tmpForMR06}" "mapper_ent_igr.py" "mapper_ent_igr.py ${j}" "reducer_ent_igr.py" "reducer_ent_igr.py"
    mr "${tmpForMR06}" "${tmpForMR07}" "mapper_max.py" "mapper_max.py" "reducer_max.py" "reducer_max.py"

    getmerge ${tmpForMR07} ${nodeFile} #igr_result.txt
    local k=$(cat ${nodeFile})

    mr "${tmpForMR01}" "${tmpForMR08}" "map_id.py" "map_id.py" "reduce_id.py" "reduce_id.py ${k}"
    getmerge ${tmpForMR08} ${fileDescription}
}

function nextLayer(){
    local nodefile=$1
    local fileDescription=$2
    local level=$3
    
    for element in $(python ${pythonUtil} -v ${level} -f ${nodeFile} -d ${fileDescription});
    do
        node=$(echo ${element} | cut -d "," -f2)

        level=$(echo ${element} | cut -d "," -f3)
        level=$(expr ${level} + 1)

        nodeFile=/home/training/erica/DT_mr/mr/node_${level}_${node}.txt
        fileDescription=/home/training/erica/DT_mr/mr/desc_${level}_${node}.txt

        outputForMR01=${outputForMR}/${node}/mr01
        outputForMR02=${outputForMR}/${node}/mr002
        outputForMR03=${outputForMR}/${node}/mr03
        outputForMR04=${outputForMR}/${node}/mr04
        outputForMR05=${outputForMR}/${node}/mr005
        outputForMR06=${outputForMR}/${node}/mr06
        outputForMR07=${outputForMR}/${node}/mr07
        outputForMR08=${outputForMR}/${node}/mr08
        inputForMR06=${outputForMR}/${node}/mr00*

        returnTreenode=$(treenode ${level} ${node} ${nodeFile} ${fileDescription} ${outputForMR01} ${outputForMR02} ${outputForMR03} ${outputForMR04} ${outputForMR05} ${outputForMR06} ${outputForMR07} ${inputForMR} ${inputForMR06} ${outputForMR08})

        ret=$?
        if [ ${ret} -ne 0 ]; then
            echo "Failed(ret=${ret}) in treenode ${level} ${node} ${nodeFile} ${fileDescription} ${outputForMR01} ${outputForMR02} ${outputForMR03} ${outputForMR04} ${outputForMR05} ${outputForMR06} ${outputForMR07} ${inputForMR} ${inputForMR06} ${outputForMR08}" 
            exit ${ret}
        fi
        nextLayer ${nodeFile} ${fileDescription} ${level}
    done 
}

#nextLayer ${nodeFile} ${fileDescription} ${level}

### start for root node ###

level=0 
node=root 

nodeFile=/home/training/erica/DT_mr/mr/node_${level}_${node}.txt
fileDescription=/home/training/erica/DT_mr/mr/desc_${level}_${node}.txt

inputForMR06=${outputForMR}/${node}/mr00*

outputForMR01=${outputForMR}/${node}/mr01
outputForMR02=${outputForMR}/${node}/mr002
outputForMR03=${outputForMR}/${node}/mr03
outputForMR04=${outputForMR}/${node}/mr04
outputForMR05=${outputForMR}/${node}/mr005
outputForMR06=${outputForMR}/${node}/mr06
outputForMR07=${outputForMR}/${node}/mr07
outputForMR08=${outputForMR}/${node}/mr08

returnTreenode=$(treenode ${level} ${node} ${nodeFile} ${fileDescription} ${outputForMR01} ${outputForMR02} ${outputForMR03} ${outputForMR04} ${outputForMR05} ${outputForMR06} ${outputForMR07} ${inputForMR} ${inputForMR06} ${outputForMR08})

ret=$?
if [ ${ret} -ne 0 ]; then
    echo "Failed(ret=${ret}) in treenode ${level} ${node} ${nodeFile} ${fileDescription} ${outputForMR01} ${outputForMR02} ${outputForMR03} ${outputForMR04} ${outputForMR05} ${outputForMR06} ${outputForMR07} ${inputForMR} ${inputForMR06} ${outputForMR08}" 
    exit ${ret}
fi 

### end of root node ###

nextLayer ${nodeFile} ${fileDescription} ${level}
