#!/bin/sh

if [ "$1" ]
then
  echo $1
  PERIOD=$1
fi

spark-submit \
    --name "ReadOrcAndExtractBidFromDsp" \
    --master yarn
    --conf "spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8" \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.yarn.max.executor.failures=10" \
    --queue irteam \
    --class driver.clicker.ReadOrcAndExtractBid \
    /home1/irteam/seungwanjo/Spark-Test-jar-with-dependencies.jar \
    ${PERIOD}