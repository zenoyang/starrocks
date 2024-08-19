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

package com.starrocks.format.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MaterializedIndexMeta {

    @JsonProperty("indexId")
    private Long indexId;

    @JsonProperty("keysType")
    private String keysType;

    @JsonProperty("columns")
    private List<Column> columns;

    @JsonProperty("schemaId")
    private long schemaId;

    @JsonProperty("sortKeyIdxes")
    public List<Integer> sortKeyIdxes = new ArrayList<>();

    @JsonProperty("sortKeyUniqueIds")
    public List<Integer> sortKeyUniqueIds = new ArrayList<>();

    @JsonProperty("schemaVersion")
    private int schemaVersion = 0;

    @JsonProperty("shortKeyColumnCount")
    private short shortKeyColumnCount;

    public MaterializedIndexMeta() {
    }

    public Long getIndexId() {
        return indexId;
    }

    public void setIndexId(Long indexId) {
        this.indexId = indexId;
    }

    public String getKeysType() {
        return keysType;
    }

    public void setKeysType(String keysType) {
        this.keysType = keysType;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(long schemaId) {
        this.schemaId = schemaId;
    }

    public List<Integer> getSortKeyIdxes() {
        return sortKeyIdxes;
    }

    public void setSortKeyIdxes(List<Integer> sortKeyIdxes) {
        this.sortKeyIdxes = sortKeyIdxes;
    }

    public List<Integer> getSortKeyUniqueIds() {
        return sortKeyUniqueIds;
    }

    public void setSortKeyUniqueIds(List<Integer> sortKeyUniqueIds) {
        this.sortKeyUniqueIds = sortKeyUniqueIds;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public short getShortKeyColumnCount() {
        return shortKeyColumnCount;
    }

    public void setShortKeyColumnCount(short shortKeyColumnCount) {
        this.shortKeyColumnCount = shortKeyColumnCount;
    }
}
