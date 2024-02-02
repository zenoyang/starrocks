// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.server.ve.emr;

import com.google.common.collect.Queues;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.credential.CloudConfigurationConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

class StorageVolumeInfo {
    String type;
    List<String> locations;
    Map<String, String> params;

    StorageVolumeInfo(String type, List<String> locations, Map<String, String> params) {
        this.type = type;
        this.locations = locations;
        this.params = params;
    }
}

public class TosBucketTagMgr extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(TosBucketTagMgr.class);

    private static class SingletonHolder {
        private static final TosBucketTagMgr INSTANCE = new TosBucketTagMgr();
    }

    public static TosBucketTagMgr getInstance() {
        return TosBucketTagMgr.SingletonHolder.INSTANCE;
    }

    private final BlockingQueue<StorageVolumeInfo> tosBucketQueue = Queues.newLinkedBlockingDeque(100);

    public TosBucketTagMgr() {
        // 2min period
        super("TosBucketTagManager", 120 * 1000L);
    }

    public void onCreateStorageVolume(String type, List<String> locations, Map<String, String> params) {
        if (Config.emr_tos_bucket_tag_enabled) {
            StorageVolumeInfo svInfo = new StorageVolumeInfo(type, locations, params);
            tosBucketQueue.add(svInfo);
            LOG.info("Add storage volume {} to tag queues.", locations.get(0));
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        StorageVolumeInfo svInfo;
        try {
            int index = 0;
            int batchSize = 10;
            while (!tosBucketQueue.isEmpty() && index < batchSize) {
                svInfo = tosBucketQueue.take();
                if (svInfo == null || svInfo.type == null || !svInfo.type.equalsIgnoreCase("s3")) {
                    continue;
                }
                tagTosBucket(svInfo);
                index++;
            }
        } catch (Throwable e) {
            LOG.error("Tag tos bucket failed.", e);
        }
    }

    private void tagTosBucket(StorageVolumeInfo svInfo) {
        TosBucketTagService service = null;
        String bucket = null;
        try {
            String accessKey = svInfo.params.get(CloudConfigurationConstants.AWS_S3_ACCESS_KEY);
            String secretKey = svInfo.params.get(CloudConfigurationConstants.AWS_S3_SECRET_KEY);

            bucket = (new URI(svInfo.locations.get(0))).getHost();
            service = new TosBucketTagService(accessKey, secretKey);
            service.putBucketTagging(bucket);
        } catch (Throwable e) {
            LOG.warn("Tag tos bucket:{} failed.", bucket, e);
        } finally {
            if (service != null) {
                service.destroy();
            }
        }
    }
}
