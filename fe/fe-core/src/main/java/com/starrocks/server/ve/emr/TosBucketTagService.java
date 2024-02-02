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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.volcengine.error.SdkError;
import com.volcengine.model.response.RawResponse;
import com.volcengine.service.BaseServiceImpl;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class TosBucketTagService extends BaseServiceImpl {
    private static final Logger LOG = LogManager.getLogger(TosBucketTagService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true);

    public TosBucketTagService(String accessKey, String secretKey) {
        this(accessKey, secretKey, null);
    }

    public TosBucketTagService(String accessKey, String secretKey, String securityToken) {
        super(TosBucketTagConfig.serviceInfo, TosBucketTagConfig.apiInfoList);
        this.setAccessKey(accessKey);
        this.setSecretKey(secretKey);
        if (!Strings.isNullOrEmpty(securityToken)) {
            this.setSessionToken(securityToken);
        }
    }


    public void putBucketTagging(String bucket) throws Exception {
        List<NameValuePair> bucketParameters = new ArrayList<NameValuePair>() {
            {
                add(new BasicNameValuePair("Bucket", bucket));
            }
        };

        RawResponse response = this.json(TosBucketTagConfig.PUT_TAG_ACTION_NAME, bucketParameters, "");
        if (response.getCode() != SdkError.SUCCESS.getNumber()) {
            LOG.warn("Tag tos bucket:{} failed with code:{}, exception:{}.",
                    bucket, response.getCode(), response.getException());
            throw response.getException();
        }
        LOG.info("Tag tos bucket:{} successful!", bucket);
    }

    public TosBucketTagSetResponse getBucketTagging(String bucket) throws Exception {
        TosBucketTagSetResponse tags = null;
        List<NameValuePair> bucketParameters = new ArrayList<NameValuePair>() {
            {
                add(new BasicNameValuePair("Bucket", bucket));
            }
        };
        RawResponse response = this.json(TosBucketTagConfig.GET_TAG_ACTION_NAME, bucketParameters, "");

        if (response.getCode() != SdkError.SUCCESS.getNumber()) {
            throw response.getException();
        } else {
            tags = MAPPER.readValue(response.getData(), TosBucketTagSetResponse.class);
        }
        return tags;
    }

}
