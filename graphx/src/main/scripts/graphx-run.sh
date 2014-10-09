#!/bin/sh

SPARK_HOME=~/spark-1.1.0-bin-hadoop2.4
DATA_PATH=~/snap/loc-gowalla_edges.txt

${SPARK_HOME}/bin/spark-submit --class com.liuzhen.graphx.SocialNetwork --master "local[2]" \
    --config "spark.executor.memory=1g" ./graphx-0.0.1-SNAPSHOT-shaded.jar ${DATA_PATH}

