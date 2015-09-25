#!/bin/bash

USERNAME=$1

if [ "$USERNAME" == "" ]; then
    echo "ERROR: No username specified in parameter."
    echo "Usage: $0 [username]"
    exit 1;
fi

cd `dirname "$0"`;


LOG_DIR=/var/nginx/log
NGINX_HOME=/var/nginx
CURRENT_TIME=`date +"%Y-%m-%d_%H-%M-%S"`
HADOOP_HOME=/usr/lib/hadoop

ETU_REC_HDFS_TMP_LOG_DIR=/user/etu/etu-recommender/tmpLog
ETU_REC_HDFS_LOG_DIR=/user/etu/etu-recommender/log
ETU_REC_HDFS_LOG_LATEST_DIR=/user/etu/etu-recommender/log/latest
NGINX_PID=`cat /var/run/nginx.pid`
LOG_FILE=$LOG_DIR/er_access.saved.$CURRENT_TIME.log
LOG_PATTERN=*.saved.*.log
mv $LOG_DIR/er_access.log $LOG_FILE
kill -USR1 $NGINX_PID

su -c "/usr/bin/hadoop fs -mkdir $ETU_REC_HDFS_LOG_DIR" $USERNAME
su -c "/usr/bin/hadoop fs -mkdir $ETU_REC_HDFS_TMP_LOG_DIR" $USERNAME
su -c "/usr/bin/hadoop fs -mkdir $ETU_REC_HDFS_LOG_LATEST_DIR" $USERNAME

su -c "/usr/bin/hadoop fs -put $LOG_DIR/$LOG_PATTERN $ETU_REC_HDFS_TMP_LOG_DIR" $USERNAME
su -c "/usr/bin/hadoop fs -mv $ETU_REC_HDFS_TMP_LOG_DIR/* $ETU_REC_HDFS_LOG_LATEST_DIR" $USERNAME

if [ "$?" == "0" ]; then
        rm -Rf $LOG_DIR/$LOG_PATTERN
fi;
