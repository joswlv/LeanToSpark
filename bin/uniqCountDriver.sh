if [ "$1" ]
then
  echo $1
  BEFORE_DATE=$1
else
  BEFORE_DATE=`date -dlast-monday +%Y%m%d`
fi

if [ "$2" ]
then
  echo $2
  NEW_DATE=$2
else
  NEW_DATE=`date -d '1 day ago' +'%Y%m%d'`
fi
DATE=${NEW_DATE}

run() {
        hdfs dfs -rmr -/${NEW_DATE}
        UNIQ_OUTPUT_PATH=-/${DATE}
        BEFORE_INPUT="-/${BEFORE_DATE}/*.gz"

        NOW_INPUT="/user/irteam/dmp/demo/data/${NEW_DATE}/*.gz"

        echo $INPUT_PATHS

        appName="getDemoUniqEDIT_vol"
        class="driver.demolog.UniqCountByCounting"
        #class="driver.demolog.UniqCountBySubtract"
        runjar="./dmp-demo-targeting-java-0.0.1-jar-with-dependencies.jar"
        configFile="./configuration.properties"
        PORTVAR=$((RANDOM/100+14040))

        echo "==>Target NEWdate is ${DATE} | BEFORE_DATE IS ${BEFORE_DATE}"
        echo "args[0] (appName) = ${appName}"
        echo "args[1] (configFile) = ${configFile}"
        echo "args[2] (input1) = ${NOW_INPUT}"
        echo "args[3] (input2) = ${BEFORE_INPUT}"
        echo "args[4] (outdir) = ${UNIQ_OUTPUT_PATH}"

        spark-submit \
        --class ${class} \
        --master yarn \
        --conf 'spark.executor.extraJavaOptions=-Dfile.encoding=UTF-8'  \
        ${runjar} \
        "${appName}" \
        "${configFile}" \
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