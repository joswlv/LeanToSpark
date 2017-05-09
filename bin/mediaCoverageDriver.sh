#!/usr/bin/env bash

run() {
        UNIQ_OUTPUT_PATH="/Users/Jo_seungwan/dev/"
        NOW_INPUT="/Users/Jo_seungwan/dev/dmp_log.log"
        BEFORE_INPUT="/Users/Jo_seungwan/dev/adxdsp.log"

        appName="MediaCoverage_TEST"
        class="driver.media_coverage.MediaCoverageByDataFrame"
        runjar="../target/Spark-Test-jar-with-dependencies.jar"

        echo "args[0] (appName) = ${appName}"
        echo "args[1] (input1) = ${NOW_INPUT}"
        echo "args[2] (input2) = ${BEFORE_INPUT}"
        echo "args[3] (outdir) = ${UNIQ_OUTPUT_PATH}"

        spark-submit \
        --class ${class} \
        --master local \
        --conf 'spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8'  \
        ${runjar} \
        "${appName}" \
        "${NOW_INPUT}" \
        "${BEFORE_INPUT}" \
        "${UNIQ_OUTPUT_PATH}"

        return $?
}

run
if [ $? -ne 0 ]; then
        echo "failfailfailfialfialfailsfialfil"
else
        echo "good success!"
        exit $?
fi