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

echo "Start minio"
mkdir -p /opt/minio/data/
mkdir -p /opt/minio/log/


# 设置密钥
export MINIO_ACCESS_KEY=minio_access_key
export MINIO_SECRET_KEY=minio_secret_key

# 后台启动
nohup minio server /opt/minio/data/ > /opt/minio/log/minio.log 2>&1 &
# 查看进程
ps -ef |grep minio
sleep 2

mc alias set mintos http://127.0.0.1:9000  minio_access_key minio_secret_key
mc mb --region --region=cn-beijing mintos/bucket1/starrocks_warehouse/  