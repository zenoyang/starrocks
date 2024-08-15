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
export STARROCKS_HOME=${ROOT}/../

echo "Start mini cluster ${STARROCKS_HOME}"
mkdir -p output/be/storage
mkdir -p output/fe/meta

echo "priority_networks = 127.0.0.1/8" >> output/be/conf/be.conf
sed -i '76s|background_thread:true|background_thread:true,tcache:false|g' output/be/bin/start_backend.sh

output/be/bin/start_be.sh --daemon
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk/
output/fe/bin/start_fe.sh --daemon

sleep 6
echo "check fe and be process"

let i=1
while [ $i -lt 30 ]; do
  sleep 2
  let i+=1
  be_ps=`ps -ef |grep starrocks_be |grep -v grep`
  echo "i = $i"
  echo "be_ps = $be_ps"
  if [ "x$be_ps" != "x" ] ;then
    echo "start be success"
    break
  else
    output/be/bin/start_be.sh --daemon
  fi
  echo "start be agian"
  sleep 2
done

if [ $i -eq 30 ];then
    echo "be process start failed, the log is:"
    tail -100 output/be/log/be.out
    exit 1
fi

fe_ps=`ps -ef |grep com.starrocks.StarRocksFE |grep -v grep`
if [ "xfe_ps" = "x" ];then
  echo "fe process start failed, the log is:"
  tail -100  output/fe/log/fe.log
  exit 1
fi

sleep 60
echo "ALTER SYSTEM ADD BACKEND '127.0.0.1:9050';"|mysql -h 127.0.0.1  -P 9030 -u root
echo "Started mini cluster"
sleep 10
exit 0

