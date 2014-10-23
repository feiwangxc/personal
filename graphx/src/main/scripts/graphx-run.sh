#!/bin/sh

SPARK_HOME=~/spark-1.1.0-bin-hadoop2.4
SOCIAL_FILE=~/snap/loc-gowalla_edges.txt
CHECKIN_FILE=~/snap/loc-gowalla_totalCheckins.txt
MAX_FRIEND_NUM=10
MAX_LOC_NUM=10

rm -fr results

${SPARK_HOME}/bin/spark-submit --class com.liuzhen.graphx.SocialNetwork --master "local[4]" \
    --conf "spark.executor.memory=1g" ./graphx-0.0.1-SNAPSHOT-shaded.jar \
    ${SOCIAL_FILE} ${CHECKIN_FILE} ${MAX_FRIEND_NUM} ${MAX_LOC_NUM}

