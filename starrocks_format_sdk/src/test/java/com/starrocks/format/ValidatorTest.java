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

import com.starrocks.format.rest.LoadNonSupportException;
import com.starrocks.format.rest.RequestException;
import com.starrocks.format.rest.Validator;
import com.starrocks.format.rest.model.TableSchema;

import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ValidatorTest extends BaseFormatTest {
    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }
    private static Stream<Arguments> trueOrFalse() {
        return Stream.of(
                Arguments.of("true"),
                Arguments.of("false")
        );
    }

    @Test
    public void testOnlyOlapTable() throws RequestException, LoadNonSupportException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_olap_table_only` (\n" +
                "  `rowId` int(11) NULL COMMENT \"\",\n" +
                "  `c_boolean` BOOLEAN NULL COMMENT \"\",\n" +
                "  `c_tinyint` TINYINT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(rowId)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(rowId) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"fast_schema_evolution\" = \"true\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        executeSql(createTableSQL);
        String tableName = "tb_olap_table_only";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Assertions.assertTrue(Validator.validateSegmentLoadExport(tableSchema));

        String createMVSQL = "create materialized view if not exists `demo`.`tb_materialized_view` \n" +
                "DISTRIBUTED BY HASH(`rowId`) \n" +
                " REFRESH ASYNC\n" +
                "AS \n" +
                "select rowId, c_tinyint from demo.tb_olap_table_only;";
        executeSql(createMVSQL);
        String mvName = "tb_materialized_view";
        final TableSchema mvSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, mvName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(mvSchema));
        Assertions.assertEquals("Only olap table support for segment load.", e.getMessage());
    }

    @Test
    public void testOnlyNormalState() throws RequestException, LoadNonSupportException {
        String dropTableSQL = "drop table if exists `demo`.`tb_normal_state_only`";
        executeSql(dropTableSQL);

        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_normal_state_only` (\n" +
                "  `rowId` int(11) NULL COMMENT \"\",\n" +
                "  `c_boolean` BOOLEAN NULL COMMENT \"\",\n" +
                "  `c_tinyint` TINYINT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(rowId)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(rowId) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        executeSql(createTableSQL);
        String tableName = "tb_normal_state_only";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Assertions.assertTrue(Validator.validateSegmentLoadExport(tableSchema));

        String createMVSQL = "create materialized view `demo`.`tb_rollup_in_state_table` \n" +
                "AS \n" +
                "select rowId, c_tinyint from demo.tb_normal_state_only;";
        executeSql(createMVSQL);
        final TableSchema tableSchema2 = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema2));
        Assertions.assertEquals("Table is in ROLLUP state, it did not support for segment load.", e.getMessage());
    }


    @Test
    public void testLargeIntColumType() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_large_int_column_type` (\n" +
                "  `rowId` int(11) NULL COMMENT \"\",\n" +
                "  `c_largeint` LARGEINT NULL COMMENT \"\",\n" +
                "  `c_tinyint` TINYINT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(rowId)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(rowId) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_large_int_column_type";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Column type: LargeInt was not support for segment load.", e.getMessage());
    }

    @Test
    public void testAutoIncrementColumType() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_auto_increment_column_type` (\n" +
                "  `rowId` int(11) NULL COMMENT \"\",\n" +
                "  `c_largeint` bigint NOT NULL AUTO_INCREMENT COMMENT \"\",\n" +
                "  `c_tinyint` TINYINT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(rowId)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(rowId) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_auto_increment_column_type";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Auto increment column was not support for segment load.", e.getMessage());
    }

    @Test
    public void testExprPartitionType() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_expr_partition_type` (\n" +
                "    event_day DATETIME NOT NULL,\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "PARTITION BY date_trunc('day', event_day)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_expr_partition_type";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Expr range partition was not support for segment load.", e.getMessage());
    }

    @Test
    public void testNoRollUp() throws RequestException, LoadNonSupportException {
        String dropTableSQL = "drop table if exists `demo`.`tb_no_rollup_table`";
        executeSql(dropTableSQL);
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_no_rollup_table` (\n" +
                "  `rowId` int(11) NULL COMMENT \"\",\n" +
                "  `c_boolean` BOOLEAN NULL COMMENT \"\",\n" +
                "  `c_tinyint` TINYINT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(rowId)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(rowId) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"fast_schema_evolution\" = \"true\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";

        executeSql(createTableSQL);
        String tableName = "tb_no_rollup_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Assertions.assertTrue(Validator.validateSegmentLoadExport(tableSchema));

        String createMVSQL = "create materialized view `demo`.`tb_rollup_in_table` \n" +
                "AS \n" +
                "select rowId, c_tinyint from demo.tb_no_rollup_table;";
        executeSql(createMVSQL);
        Assertions.assertTrue(waitCreateRollupFinished(tableName));
        final TableSchema tableSchema2 = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema2));
        Assertions.assertEquals("Rollup was not support for segment load.", e.getMessage());
    }

    private boolean waitCreateRollupFinished(String table) {
        String sql = "SHOW ALTER TABLE ROLLUP FROM demo;";
        try {
            Thread.sleep(5000);
            String state;
            long timeout = 120000;
            long starTime = System.currentTimeMillis() / 1000;
            do {
                List<Map<String, String>> alters = executeSqlWithReturn(sql, new ArrayList<>());
                if (alters.isEmpty()) {
                    System.out.println("No alters");
                    return false;
                }
                // loads only have one row
                for (Map<String, String> l : alters) {
                    state = l.get("State");
                    if (!l.get("TableName").equalsIgnoreCase(table)) {
                        continue;
                    }
                    if (state.equalsIgnoreCase("CANCELLED")) {
                        System.out.println("Add roll up had failed with error: " + l.get("ErrorMsg"));
                        return false;
                    } else if (state.equalsIgnoreCase("Finished")) {
                        return true;
                    } else {
                        System.out.println("Add roll up had not finished, try another loop with state = " + state);
                    }
                }
                Thread.sleep(2000);
            } while ((System.currentTimeMillis() / 1000 - starTime) < timeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    @Test
    public void testColocateTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_colocate_table` (\n" +
                "    event_day DATETIME NOT NULL,\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"colocate_with\" = \"group1212\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_colocate_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Colocate group was not support for segment load.", e.getMessage());
    }

    // not support in 3.2.3
    @Ignore
    public void testHybridColumRowTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_hybrid_column_row_table` (\n" +
                "    event_day DATETIME NOT NULL,\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "PRIMARY KEY(event_day, site_id, city_code, user_name)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"storage_type\" = \"storage_type\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_hybrid_column_row_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Hybrid row-column storage was not support for segment load.", e.getMessage());
    }

    @Test
    public void testUniqueConstraintsTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_unique_constrains_table` (\n" +
                "    event_day DATETIME NOT NULL,\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "PRIMARY KEY(event_day, site_id, city_code, user_name)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"unique_constraints\" = \"site_id,user_name\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_unique_constrains_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Unique constraints was not support for segment load.", e.getMessage());
    }

    @Test
    public void testForeignKeyConstraintsTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_foreign_key_constrains_table1` (\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "PRIMARY KEY(site_id)\n" +
                "DISTRIBUTED BY HASH(site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"unique_constraints\" = \"site_id\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);

        createTableSQL = "CREATE TABLE if not exists `demo`.`tb_foreign_key_constrains_table2` (\n" +
                "    event_day DATETIME NOT NULL,\n" +
                "    site_id INT NOT NULL DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "PRIMARY KEY(event_day, site_id, city_code, user_name)\n" +
                "DISTRIBUTED BY HASH(event_day, site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"foreign_key_constraints\" = \"(site_id) REFERENCES tb_foreign_key_constrains_table1(site_id)\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_foreign_key_constrains_table2";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Foreign key constraints was not support for segment load.", e.getMessage());
    }

    @Test
    public void testBloomFilterTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_bloom_filter_table` (\n" +
                "    site_id INT DEFAULT '10',\n" +
                "    city_code VARCHAR(100),\n" +
                "    user_name VARCHAR(32) DEFAULT '',\n" +
                "    pv BIGINT DEFAULT '0'\n" +
                ")\n" +
                "PRIMARY KEY( site_id)\n" +
                "DISTRIBUTED BY HASH(site_id) buckets 1\n" +
                "PROPERTIES (\n" +
                " \"bloom_filter_columns\" = \"user_name\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_bloom_filter_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Bloom filter columns was not support for segment load.", e.getMessage());
    }

    @Test
    public void testBitmapTable() throws RequestException {
        String createTableSQL = "CREATE TABLE if not exists `demo`.`tb_bitmap_table` (\n" +
                "  `lo_orderkey` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_orderdate` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_orderpriority` varchar(16) NOT NULL COMMENT \"\",\n" +
                "  `lo_quantity` int(11) NOT NULL COMMENT \"\",\n" +
                "  `lo_revenue` int(11) NOT NULL COMMENT \"\",\n" +
                "   INDEX lo_orderdate_index (lo_orderdate) USING BITMAP\n" +
                ") ENGINE=OLAP \n" +
                "DUPLICATE KEY(`lo_orderkey`)\n" +
                "DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        executeSql(createTableSQL);
        String tableName = "tb_bitmap_table";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        LoadNonSupportException e = Assertions.assertThrows(LoadNonSupportException.class,
                () -> Validator.validateSegmentLoadExport(tableSchema));
        Assertions.assertEquals("Bitmap index was not support for segment load.", e.getMessage());
    }
}
