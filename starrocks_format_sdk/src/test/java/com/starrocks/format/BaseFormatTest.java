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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BaseFormatTest {
    // Driver name for mysql connector 5.1 which is deprecated in 8.0
    private static final String MYSQL_51_DRIVER_NAME = "com.mysql.jdbc.Driver";
    // Driver name for mysql connector 8.0
    private static final String MYSQL_80_DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_SITE_URL = "https://dev.mysql.com/downloads/connector/j/";
    private static final String MAVEN_CENTRAL_URL = "https://repo1.maven.org/maven2/mysql/mysql-connector-java/";

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
        ColumnPB.Builder builder =  ColumnPB.newBuilder()
                .setUniqueId(column.getUniqueId())
                .setName(column.getName())
                .setType(column.getPrimitiveType())
                .setIsKey(Boolean.TRUE.equals(column.getKey()))
                .setAggregation(Optional.ofNullable(column.getAggregationType()).orElse("NONE"))
                .setIsNullable(Boolean.TRUE.equals(column.getAllowNull()))
                // .setDefaultValue(ByteString.copyFrom(column.getDefaultValue(), StandardCharsets.UTF_8))
                .setLength(stringLength)
                .setIndexLength(stringLength)
                .setUniqueId(column.getUniqueId())
                .setIsAutoIncrement(Boolean.TRUE.equals(column.getAutoIncrement()));
        if (column.getPrecision() != null) {
            builder.setPrecision(column.getPrecision());
        }
        if (column.getScale() != null) {
            builder.setFrac(column.getScale());
        }
        return builder.build();
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


    protected List<Map<String, String>> executeSqlWithReturn(String sqlPattern, List<String> parameters) {
        List<Map<String, String>> columnValues = new ArrayList<>();
        try (
                Connection conn = createJdbcConnection();
                PreparedStatement ps = conn.prepareStatement(sqlPattern)
        ) {
            for (int i = 1; i <= parameters.size(); i++) {
                ps.setObject(i, parameters.get(i - 1));
            }

            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    // colName -> colValue
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            rs.close();
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw (IllegalStateException) e;
            }
            throw new IllegalStateException("extract column values by sql error, " + e.getMessage(), e);
        }

        return columnValues;
    }

    protected void executeSql(String sqlStatement) {
        try (
                Connection conn = createJdbcConnection();
                Statement stmt = conn.createStatement();
        ) {
            stmt.execute(sqlStatement);
        } catch (Exception e) {
            if (e instanceof IllegalStateException) {
                throw (IllegalStateException) e;
            }
            throw new IllegalStateException("submit sql error, " + e.getMessage(), e);
        }
    }

    protected Connection createJdbcConnection() {
        try {
            Class.forName(MYSQL_80_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            try {
                Class.forName(MYSQL_51_DRIVER_NAME);
            } catch (ClassNotFoundException ie) {
                String msg = String.format("Can't find mysql jdbc driver, please download it and " +
                                "put it in your classpath manually. Note that the connector does not include " +
                                "the mysql driver since version 1.1.1 because of the limitation of GPL license " +
                                "used by the driver. You can download it from MySQL site %s, or Maven Central %s",
                        MYSQL_SITE_URL, MAVEN_CENTRAL_URL);
                throw new RuntimeException(msg);
            }
        }

        try {
            return DriverManager.getConnection(settings.getSrFeJdbcUrl(), settings.getSrUser(),
                    settings.getSrPassword());
        } catch (SQLException e) {
            throw new RuntimeException(settings.getSrFeJdbcUrl(), e);
        }
    }

    protected boolean waitAlterTableColumnFinished(String table) {
        String sql = String.format("SHOW ALTER TABLE COLUMN FROM demo " +
                "WHERE TableName = \"%s\" ORDER BY CreateTime DESC LIMIT 1;", table);
        try {
            Thread.sleep(2000);
            String state;
            long timeout = 60000;
            long starTime = System.currentTimeMillis() / 1000;
            do {
                List<Map<String, String>> alters = executeSqlWithReturn(sql, new ArrayList<>());
                if (alters.isEmpty()) {
                    return false;
                }
                // loads only have one row
                for (Map<String, String> l : alters) {
                    state = l.get("State");
                    if (state.equalsIgnoreCase("CANCELLED")) {
                        System.out.println("Alter column had failed with error: " + l.get("ErrorMsg"));
                        return false;
                    } else if (state.equalsIgnoreCase("Finished")) {
                        return true;
                    } else {
                        System.out.println("Alter column had not finished, try another loop with state = " + state);
                    }
                }
                Thread.sleep(2000);
            } while ((System.currentTimeMillis() / 1000 - starTime) < timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
}
