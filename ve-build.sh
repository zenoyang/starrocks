#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT"; pwd`

# update staros dependencies
if [ ! -z $CUSTOM_staros_version ]; then
    bvc clone data/emr/staros_3_1 /tmp/starlet --version $CUSTOM_staros_version
    mv /var/local/thirdparty/installed/starlet/ /var/local/thirdparty/installed/starlet_bak
    mv /tmp/starlet/starlet /var/local/thirdparty/installed/starlet
fi

# build fe and be
mkdir -p /root/.m2 && cp ./docker/ve-emr/settings.xml  ~/.m2/settings.xml
export BUILD_TYPE=Release && export STARROCKS_VERSION=3.2.3-ve-3 && ./build.sh --fe --be --spark-dpp --use-staros

# build broker
STARROCKS_OUTPUT=${ROOT}/output/
BROKER_HOME=${ROOT}/fs_brokers/apache_hdfs_broker/
cd ${BROKER_HOME}
./build.sh

cp -r ${BROKER_HOME}/output/apache_hdfs_broker ${STARROCKS_OUTPUT}/
