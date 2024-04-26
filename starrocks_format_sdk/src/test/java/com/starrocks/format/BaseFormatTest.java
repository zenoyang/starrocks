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

package com.starrocks.format;

import com.starrocks.format.rest.RestClient;
import com.starrocks.format.rest.model.Column;
import com.starrocks.format.rest.model.MaterializedIndexMeta;
import com.starrocks.format.rest.model.TableSchema;
import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.KeysType;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import com.starrocks.proto.Types;
import org.junit.jupiter.api.BeforeAll;

import java.util.Optional;

public class BaseFormatTest {

    protected static final String DEFAULT_CATALOG = "default_catalog";

    protected static final String DB_NAME = "demo";

    protected static final String STARROCKS_FORMAT_QUERY_PLAN = "starrocks.format.query_plan";
    protected static final String STARROCKS_FORMAT_USING_COLUMN_UID = "starrocks.format.using_column_uid";

    protected static ConnSettings settings;

    protected static RestClient restClient;

    @BeforeAll
    public static void init() throws Exception {
        settings = ConnSettings.newInstance();

        restClient = new RestClient.Builder()
                .setFeEndpoints(settings.getSrFeHttpUrl())
                .setUsername(settings.getSrUser())
                .setPassword(settings.getSrPassword())
                .build();
    }

    protected static TabletSchemaPB toPbTabletSchema(TableSchema tableSchema) {
        MaterializedIndexMeta indexMeta = tableSchema.getIndexMetas().get(0);
        TabletSchemaPB.Builder builder = TabletSchemaPB.newBuilder()
                .setId(indexMeta.getIndexId())
                .setKeysType(KeysType.valueOf(indexMeta.getKeysType()))
                .setCompressionType(Types.CompressionTypePB.LZ4);

        for (int i = 0; i < indexMeta.getColumns().size(); i++) {
            Column column = indexMeta.getColumns().get(i);
            // FIXME use uniqueId
            builder.addColumn(toPbColumn(column));
        }

        return builder.build();
    }

    protected static ColumnPB toPbColumn(Column column) {
        int stringLength = columnLength(column);
        return ColumnPB.newBuilder()
                .setUniqueId(column.getUniqueId())
                .setName(column.getName())
                .setType(column.getPrimitiveType())
                .setIsKey(Boolean.TRUE.equals(column.getKey()))
                .setAggregation(Optional.ofNullable(column.getAggregationType()).orElse("NONE"))
                .setIsNullable(Boolean.TRUE.equals(column.getAllowNull()))
                // .setDefaultValue(ByteString.copyFrom(column.getDefaultValue(), StandardCharsets.UTF_8))
                .setPrecision(column.getPrecision())
                .setFrac(column.getScale())
                .setLength(stringLength)
                .setIndexLength(stringLength)
                .setIsAutoIncrement(Boolean.TRUE.equals(column.getAutoIncrement()))
                .build();
    }

    private static int columnLength(Column column) {
        switch (column.getPrimitiveType()) {
            case "BOOLEAN":
            case "DATE":
            case "DATETIME":
                return column.getPrimitiveTypeSize();
            default:
                return column.getColumnSize() == null ? 0 : column.getColumnSize();
        }
    }

    protected static class ColumnType {

        private DataType dataType;
        private Integer length;
        private Integer precision;
        private Integer scale;

        public ColumnType(DataType dataType, Integer length) {
            this.dataType = dataType;
            this.length = length;
        }

        public ColumnType(DataType dataType, Integer length, Integer precision, Integer scale) {
            this.dataType = dataType;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }

        public DataType getDataType() {
            return dataType;
        }

        public void setDataType(DataType dataType) {
            this.dataType = dataType;
        }

        public Integer getLength() {
            return length;
        }

        public void setLength(Integer length) {
            this.length = length;
        }

        public Integer getPrecision() {
            return precision;
        }

        public void setPrecision(Integer precision) {
            this.precision = precision;
        }

        public Integer getScale() {
            return scale;
        }

        public void setScale(Integer scale) {
            this.scale = scale;
        }
    }

}
