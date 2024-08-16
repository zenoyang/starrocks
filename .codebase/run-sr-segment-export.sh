#!/usr/bin/env bash

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

export STARROCKS_HOME=${ROOT}/../
export UDF_RUNTIME_DIR=${STARROCKS_HOME}/

export BUILD_TYPE=Debug
echo "STARROCKS_HOME:" $STARROCKS_HOME
echo "----------start run build.sh ..."
sh ${STARROCKS_HOME}/build.sh --fe --be --use-staros -j 20

echo "----------start run start_minio.sh..."
sh ${STARROCKS_HOME}/.codebase/start_minio.sh

echo "----------start run start_share_nothing_mini_cluster.sh..."
sh ${STARROCKS_HOME}/.codebase/start_share_nothing_mini_cluster.sh
exit_code=`echo $?`
if [ $exit_code -ne 0 ];then
  echo "----------run start_share_nothing_mini_cluster.sh failed ...."
  exit 1
fi

echo "----------config cluster ..."
mysql -h 127.0.0.1  -P 9030 -u root < .codebase/setup_share_nothing.sql

echo "----------start run starrocks_format_sdk..."
cd ${STARROCKS_HOME}/starrocks_format_sdk
export FLAGS_logtostderr=1;
export LD_LIBRARY_PATH=/var/local/thirdparty/installed/hadoop/lib/native/
mvn test -Pformat-lib -Dtest=SegmentExportTest
exit_code=`echo $?`
if [ $exit_code -ne 0 ];then
  echo "----------run starrocks_format_sdk failed ...."
  cat ${STARROCKS_HOME}/output/be/log/be.out
  exit 1
fi

cd ${STARROCKS_HOME}
echo "----------start stop_mini_cluster.sh..."
sh ${STARROCKS_HOME}/.codebase/stop_mini_cluster.sh
