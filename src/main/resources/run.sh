#!/usr/bin/env bash

# configurable env vars
RUN_DIR=${RUN_DIR:-.}
MAIN_CLASS=${MAIN_CLASS:-spark_etl.CLI}
HADOOP_VSN=${HADOOP_VSN:-2.7.3}
SPARK_JARS=${SPARK_JARS:-/opt/spark/spark-2.1.0-bin-hadoop2.7/jars}
HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-/etc/hadoop/conf}
PACKAGE_LOGS=${PACKAGE_LOGS:-false}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

OTHER_CP=$SPARK_JARS/hadoop-hdfs-${HADOOP_VSN}.jar:$SPARK_JARS/hadoop-common-${HADOOP_VSN}.jar:$HADOOP_CONF_DIR
SPARK_STORAGE_LEVEL=MEMORY_AND_DISK_SER_2
SPARK_NUM_EXECUTORS=250
SPARK_EXECUTOR_MEMORY=7G
SPARK_EXECUTOR_CORES=2
SPARK_HOME=/opt/spark/spark-2.1.0-bin-hadoop2.7
#export YARN_CONF_DIR=$HADOOP_CONF_DIR
JAR=$(ls $RUN_DIR/*-assembly*.jar)
RT=$(date --date="6:00 today" --iso-8601=seconds | cut -f1 -d'+')
TIMESTAMP="${RT}Australia/Sydney"

CMD="$SPARK_HOME/bin/spark-submit \
--conf spark.debug.maxToStringFields=1024 \
--conf spark.driver.extraJavaOptions='-XX:PermSize=512m -XX:MaxPermSize=512m' \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.yarn.max.executor.failures=200 \
--conf spark.driver.memory=7G \
--conf spark.driver.maxResultSize=4G \
--conf spark.sql.warehouse.dir=hdfs://nameservice1/user/hive/warehouse \
--conf spark.locality.wait=0 \
--conf spark.io.compression.codec=org.apache.spark.io.SnappyCompressionCodec \
--conf spark.rdd.compress=true \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.sql.parquet.compression.codec=snappy \
--conf spark.sql.inMemoryColumnarStorage.compressed=true \
--conf spark.sql.inMemoryColumnarStorage.batchSize=100000 \
--conf spark.sql.crossJoin.enabled=true \
--conf spark.task.maxFailures=20 \
--master yarn \
--deploy-mode cluster \
--num-executors $SPARK_NUM_EXECUTORS \
--executor-memory $SPARK_EXECUTOR_MEMORY \
--executor-cores $SPARK_EXECUTOR_CORES \
--class $MAIN_CLASS \
$JAR"

green="\033[32m"
red="\033[31m"
bold="\033[1m"
reset="\033[0m"

log_green() {
  echo -e "${green}$@${reset}"
}

log_bold() {
  echo -e "${bold}$@${reset}"
}

log_red() {
  echo -e "${red}$@${reset}"
}

check_package_logs() {
  if [ "$PACKAGE_LOGS" == "true" ]
  then
    log_bold "...Log packaging enabled"
  else
    log_bold "...Log packaging disabled"
  fi
}

package_logs() {
  if [ "$PACKAGE_LOGS" == "true" ]
  then
    local_log_file=$1
    app_id=`cat $local_log_file | grep 'Submitting application application_[0-9]*_[0-9]*' | sed -r 's/.*(application_[0-9]*_[0-9]*).*/\1/g'`
    if [ -z "$app_id" ]
    then
      log_red "No application_XXX_YYY found in local log $local_log_file!"
      exit 11
    else
      log_bold "Packaging logs for $app_id after a 10 sec sleep"
      sleep 10
      mkdir -p logs/$app_id
      rm -rf logs/current
      ln -s $app_id logs/current
      cp $local_log_file logs/$app_id/$app_id.local.log
      yarn logs --applicationId $app_id > logs/$app_id/$app_id.remote.log
      cd logs/$app_id
      zip logs_$app_id.zip *.log
      cd ../..
      log_bold "Logs available at logs/$app_id/logs_$app_id.zip"
    fi
  fi
}

usage() {
  log_bold "  Usage:"
  log_bold "    help"
  log_bold "    validate-local"
  log_bold "    validate-remote"
  log_bold "    transform"
  log_bold "    extract-check"
  log_bold "    transform-check"
  exit 1
}

set -e
trail_arg="${@: -1}"
if [[ $# -lt 1 || "$trail_arg" == "help" ]]; then usage;  fi
case "$trail_arg" in
  "validate-local")
    log_bold "Validating local configuration..."
    java -cp $JAR:$OTHER_CP $MAIN_CLASS $@
    ;;
  "validate-remote")
    log_bold "Validating remote aspects..."
    java -cp $JAR:$OTHER_CP $MAIN_CLASS $@
    ;;
  "transform")
    log_bold "Run and persist transform..."
    check_package_logs
    YARN_CONF_DIR=$HADOOP_CONF_DIR eval $CMD $@ 2>&1 | tee .current_local.log
    package_logs .current_local.log
    ;;
  "extract-check")
    log_bold "Run extract check..."
    check_package_logs
    YARN_CONF_DIR=$HADOOP_CONF_DIR eval $CMD $@ 2>&1 | tee .current_local.log
    package_logs .current_local.log
    ;;
  "transform-check")
    log_bold "Run transform check..."
    check_package_logs
    YARN_CONF_DIR=$HADOOP_CONF_DIR eval $CMD $@ 2>&1 | tee .current_local.log
    package_logs .current_local.log
    ;;
  *)
    log_red "Not a valid command: $@"
    usage
    ;;
esac
log_green "$trail_arg done!"
