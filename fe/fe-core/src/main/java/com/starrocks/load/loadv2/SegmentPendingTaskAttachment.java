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

package com.starrocks.load.loadv2;

import com.google.common.collect.Maps;
import com.starrocks.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SegmentPendingTaskAttachment extends TaskAttachment {
    // { tableId/partitionId/indexId/tabletId -> [(FilePath, FileSize),...] }
    private final Map<String, List<Pair<String, Long>>> tabletMetaToDataFileInfo = Maps.newHashMap();
    // { tableId/partitionId/indexId/tabletId -> FilePath }
    private final Map<String, String> tabletMetaToSchemaFilePath = Maps.newHashMap();

    private String rootPath;

    public SegmentPendingTaskAttachment(long taskId) {
        super(taskId);
    }

    public void addDataFileInfo(String tabletMeta, Pair<String, Long> fileInfo) {
        if (!tabletMetaToDataFileInfo.containsKey(tabletMeta)) {
            tabletMetaToDataFileInfo.put(tabletMeta, new ArrayList<>());
        }
        tabletMetaToDataFileInfo.get(tabletMeta).add(fileInfo);
    }

    public void addSchemaFilePath(String tabletMeta, String filePath) {
        tabletMetaToSchemaFilePath.put(tabletMeta, filePath);
    }

    public Map<String, List<Pair<String, Long>>> getTabletMetaToDataFileInfo() {
        return tabletMetaToDataFileInfo;
    }

    public Map<String, String> getTabletMetaToSchemaFilePath() {
        return tabletMetaToSchemaFilePath;
    }

    public String getRootPath() {
        return rootPath;
    }

    public void setRootPath(String rootPath) {
        this.rootPath = rootPath;
    }
}