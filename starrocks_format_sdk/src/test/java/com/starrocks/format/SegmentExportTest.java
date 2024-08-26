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
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TableSchema;
import com.starrocks.proto.TabletSchema;
import com.starrocks.proto.Types;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SegmentExportTest extends BaseFormatTest {
    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
        settings.setSegmentExportEnabled("share_nothing");
    }

    private static Stream<Arguments> testAllPrimitiveType() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate"),
                Arguments.of("tb_all_primitivetype_write_unique"),
                Arguments.of("tb_all_primitivetype_write_aggregate"),
                Arguments.of("tb_all_primitivetype_write_primary")
        );
    }

    private static Stream<Arguments> testAllPrimitiveTypeTwice() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate2"),
                Arguments.of("tb_all_primitivetype_write_unique2"),
                Arguments.of("tb_all_primitivetype_write_aggregate2"),
                Arguments.of("tb_all_primitivetype_write_primary2")
        );
    }

    private static Stream<Arguments> testSchemaChangeTable() {
        return Stream.of(
                Arguments.of("tb_fast_schema_change_table"),
                Arguments.of("tb_no_fast_schema_change_table")
        );
    }

    private static Stream<Arguments> testCompressTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_compress_lz4"),
                Arguments.of("tb_all_primitivetype_compress_zstd"),
                Arguments.of("tb_all_primitivetype_compress_zlib"),
                Arguments.of("tb_all_primitivetype_compress_snappy")
        );
    }

    private static Stream<Arguments> testSortByTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_order_by_int"),
                Arguments.of("tb_all_primitivetype_order_by_varchar")
        );
    }

    private static Stream<Arguments> testSwapTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_swap_table"),
                Arguments.of("tb_all_primitivetype_swap_diff_table")
        );
    }

    private static Stream<Arguments> testStorageTypeTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_storage_hdd")
        );
    }


    private static Stream<Arguments> testPartitionTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_partition_dup_table"),
                Arguments.of("tb_all_primitivetype_partition_primary_table")
        );
    }

    private static Stream<Arguments> testListPartitionTable() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_list_partition_dup_table"),
                Arguments.of("tb_all_primitivetype_list_partition_primary_table")
        );
    }

    @ParameterizedTest
    @MethodSource("testAllPrimitiveType")
    public void testAllPrimitiveType(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testAllPrimitiveTypeTwice")
    public void testAllPrimitiveTypeTwice(String tableName) throws Exception {
        int times = 2;
        for (int i = 0; i < times; i++) {
            String uuid = RandomStringUtils.randomAlphabetic(8);
            String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
            TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
            Validator.validateSegmentLoadExport(tableSchema);
            TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

            List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME,
                    tableName, false);
            long tableId = tableSchema.getId();
            long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

            assertFalse(partitions.isEmpty());
            String fastSchemaChange = tableSchema.isFastSchemaChange();
            for (TablePartition partition : partitions) {
                List<TablePartition.Tablet> tablets = partition.getTablets();
                assertFalse(tablets.isEmpty());

                for (TablePartition.Tablet tablet : tablets) {
                    Long tabletId = tablet.getId();
                    try {
                        String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                                + indexId + "/" + tabletId;
                        Map<String, String> options = settings.toMap();
                        if (fastSchemaChange.equalsIgnoreCase("false")) {
                            // http://10.37.55.121:8040/api/meta/header/15143
                            String metaUrl = tablet.getMetaUrls().get(0);
                            String metaContext = restClient.getTabletMeta(metaUrl);
                            options.put("starrocks.format.metaContext", metaContext);
                            options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                        }
                        StarRocksWriter writer = new StarRocksWriter(tabletId,
                                tabletSchema,
                                -1L,
                                storagePath,
                                options);
                        writer.open();
                        // write use chunk interface
                        Chunk chunk = writer.newChunk(4096);

                        chunk.reset();
                        fillSampleData(tabletSchema, chunk, 0, 200);
                        writer.write(chunk);

                        chunk.reset();
                        fillSampleData(tabletSchema, chunk, 200, 200);
                        writer.write(chunk);

                        chunk.release();
                        writer.flush();
                        writer.finish();
                        writer.close();
                        writer.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail();
                    }
                }
            }

            String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
            String ak = settings.getS3AccessKey();
            String sk = settings.getS3SecretKey();
            String endpoint = settings.getS3Endpoint();

            loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
            boolean res = waitUtilLoadFinished(DB_NAME, label);
            assertTrue(res);
        }

        List<Map<String, String>> outputs = getTableRowNum(DB_NAME, tableName);
        assertEquals(1, outputs.size());
        switch (tableName) {
            case "tb_all_primitivetype_write_duplicate2":
                assertEquals(800, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_unique2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_aggregate2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_primary2":
                assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));
                break;
            default:
                fail();
        }

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(DB_NAME, tableName);
        assertEquals(1, outputsNotNull.size());
        switch (tableName) {
            case "tb_all_primitivetype_write_duplicate2":
                assertEquals(798, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_unique2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_aggregate2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            case "tb_all_primitivetype_write_primary2":
                assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
                break;
            default:
                fail();
        }
    }

    @ParameterizedTest
    @MethodSource("testSchemaChangeTable")
    public void testSchemaChangeTable(String tableName) throws Exception {
        String dropColumnTable = String.format("alter table `demo`.`%s` DROP COLUMN c_bigint",tableName);
        executeSql(dropColumnTable);
        Assertions.assertTrue(waitAlterTableColumnFinished(tableName));

        String addColumnTable = String.format("alter table `demo`.`%s` ADD COLUMN new_col INT" +
                " DEFAULT \"0\" AFTER c_datetime", tableName);
        executeSql(addColumnTable);
        Assertions.assertTrue(waitAlterTableColumnFinished(tableName));
        testTableTypes(tableName);
    }

    @Test
    public void testRenameTable() throws Exception {
        String tableName = "tb_all_primitivetype_rename_table";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 0, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String renameSql = String.format("alter table `demo`.`%s` rename tb_all_primitivetype_rename_table2",tableName);
        executeSql(renameSql);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        IllegalStateException illegalStateException = Assertions.assertThrows(IllegalStateException.class,
                ()->loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint));
        Assertions.assertEquals("submit sql error, Getting analyzing error." +
                " Detail message: Table demo.tb_all_primitivetype_rename_table is not found.",
                illegalStateException.getMessage());
    }

    @Test
    public void testModifyTableComment() throws Exception {
        String tableName = "tb_all_primitivetype_modify_comment";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 0, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String modifyComment = String.format("alter table `demo`.`%s` COMMENT = \"this is test table\"",tableName);
        executeSql(modifyComment);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(DB_NAME, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNum(DB_NAME, tableName);
        assertEquals(1, outputs.size());
        assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(DB_NAME, tableName);
        assertEquals(1, outputsNotNull.size());
        assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
    }

    @ParameterizedTest
    @MethodSource("testSwapTable")
    public void testSwapTable(String tableName) throws Exception {
        String swapTableName = "tb_all_primitivetype_swap_table" + "1";

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 0, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String renameSql = String.format("alter table `demo`.`%s` swap with %s",tableName, swapTableName);
        executeSql(renameSql);
        Thread.sleep(1000);

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(DB_NAME, label);
        assertFalse(res);
    }

    @ParameterizedTest
    @MethodSource("testCompressTypeTable")
    public void testCompressTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testSortByTypeTable")
    public void testSortByTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    @ParameterizedTest
    @MethodSource("testStorageTypeTable")
    public void testStorageTypeTable(String tableName) throws Exception {
        testTableTypes(tableName);
    }

    private void testTableTypes(String tableName) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 0, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(DB_NAME, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNum(DB_NAME, tableName);
        assertEquals(1, outputs.size());
        assertEquals(400, Integer.valueOf(outputs.get(0).get("num")));

        List<Map<String, String>> outputsNotNull = getTableRowNumNotNUll(DB_NAME, tableName);
        assertEquals(1, outputsNotNull.size());
        assertEquals(399, Integer.valueOf(outputsNotNull.get(0).get("num")));
    }

    @Test
    public void testWriteLocalUseChunkForBulkLoad(@TempDir Path tempDir) throws Exception {
        TabletSchema.TabletSchemaPB.Builder schemaBuilder = TabletSchema.TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(TabletSchema.KeysType.DUP_KEYS)
                .setCompressionType(Types.CompressionTypePB.LZ4_FRAME);

        BaseFormatTest.ColumnType[] columnTypes = new BaseFormatTest.ColumnType[] {
                new BaseFormatTest.ColumnType(DataType.INT, 4),
                new BaseFormatTest.ColumnType(DataType.BOOLEAN, 4),
                new BaseFormatTest.ColumnType(DataType.TINYINT, 1),
                new BaseFormatTest.ColumnType(DataType.SMALLINT, 2),
                new BaseFormatTest.ColumnType(DataType.BIGINT, 8),
                new BaseFormatTest.ColumnType(DataType.LARGEINT, 16),
                new BaseFormatTest.ColumnType(DataType.FLOAT, 4),
                new BaseFormatTest.ColumnType(DataType.DOUBLE, 8),
                new BaseFormatTest.ColumnType(DataType.DATE, 4),
                new BaseFormatTest.ColumnType(DataType.DATETIME, 8),
                new BaseFormatTest.ColumnType(DataType.DECIMAL32, 8),
                new BaseFormatTest.ColumnType(DataType.DECIMAL64, 16),
                new BaseFormatTest.ColumnType(DataType.VARCHAR, 32 + Integer.SIZE)};

        int colId = 0;
        for (BaseFormatTest.ColumnType columnType : columnTypes) {
            TabletSchema.ColumnPB.Builder columnBuilder = TabletSchema.ColumnPB.newBuilder()
                    .setName("c_" + columnType.getDataType())
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(true)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            if (columnType.getDataType().getLiteral().startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (columnType.getDataType().getLiteral().startsWith("DECIMAL128")) {
                columnBuilder.setPrecision(38);
                columnBuilder.setFrac(4);
            }
            schemaBuilder.addColumn(columnBuilder.build());
            colId++;
        }
        TabletSchema.TabletSchemaPB schema = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index alway the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long tabletId = 100L;
        long txnId = 4;

        String tabletRootPath = tempDir.toAbsolutePath().toString();
        File dir = new File(tabletRootPath + "/data");
        assertTrue(dir.mkdirs());

        Map<String,String> config = new HashMap<>();
        config.put("starrocks.format.mode", "share_nothing");
        StarRocksWriter writer = new StarRocksWriter(tabletId,
                schema,
                txnId,
                tabletRootPath,
                config);
        writer.open();

        // write use chunk interface
        Chunk chunk = writer.newChunk(5);
        fillSampleData(schema, chunk, 0, 5);
        writer.write(chunk);

        chunk.release();
        writer.flush();
        writer.finish();
        writer.close();
        writer.release();
        File tabletSchemaFile= new File(tabletRootPath + "/tablet.schema");
        assertTrue(tabletSchemaFile.exists());
        assertEquals(2, Objects.requireNonNull(dir.listFiles()).length);
        for (File f: Objects.requireNonNull(dir.listFiles())) {
            assertTrue(f.toString().endsWith(".dat") || f.toString().endsWith(".pb"));
        }
    }

    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=4 fill the base value * rowId * sign.
    private static void fillSampleData(TabletSchema.TabletSchemaPB pbSchema, Chunk chunk, int startRowId, int numRows) {

        for (int colIdx = 0; colIdx < pbSchema.getColumnCount(); colIdx++) {
            TabletSchema.ColumnPB pbColumn = pbSchema.getColumn(colIdx);
            Column column = chunk.getColumn(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("rowid".equalsIgnoreCase(pbColumn.getName())) {
                    column.appendInt(rowId);
                    continue;
                }
                if (rowId == 2) {
                    column.appendNull();
                    continue;
                }
                int sign = (rowId % 2 == 0) ? -1 : 1;

                DataType dataType = DataType.fromLiteral(pbColumn.getType()).get();
                switch (dataType) {
                    case BOOLEAN:
                        column.appendBool(rowId % 2 == 0);
                        break;
                    case TINYINT:
                        if (rowId == 0) {
                            column.appendByte(Byte.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendByte(Byte.MIN_VALUE);
                        } else {
                            column.appendByte((byte) (rowId * sign));
                        }
                        break;
                    case SMALLINT:
                        if (rowId == 0) {
                            column.appendShort(Short.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendShort(Short.MIN_VALUE);
                        } else {
                            column.appendShort((short) (rowId * 10 * sign));
                        }
                        break;
                    case INT:
                        if (rowId == 0) {
                            column.appendInt(Integer.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendInt(Integer.MIN_VALUE);
                        } else {
                            column.appendInt(rowId * 100 * sign);
                        }
                        break;
                    case BIGINT:
                        if (rowId == 0) {
                            column.appendLong(Long.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendLong(Long.MIN_VALUE);
                        } else {
                            column.appendLong(rowId * 1000L * sign);
                        }
                        break;
                    case LARGEINT:
                        if (rowId == 0) {
                            column.appendLargeInt(new BigInteger("6693349846790746512344567890123456789"));
                        } else if (rowId == 1) {
                            column.appendLargeInt(new BigInteger("-6693349846790746512344567890123456789"));
                        } else {
                            column.appendLargeInt(BigInteger.valueOf(rowId * 10000L * sign));
                        }
                        break;
                    case FLOAT:
                        column.appendFloat(123.45678901234f * rowId * sign);
                        break;
                    case DOUBLE:
                        column.appendDouble(23456.78901234 * rowId * sign);
                        break;
                    case DECIMAL:
                        // decimal v2 type
                        BigDecimal bdv2;
                        if (rowId == 0) {
                            bdv2 = new BigDecimal("-12345678901234567890123.4567");
                        } else if (rowId == 1) {
                            bdv2 = new BigDecimal("999999999999999999999999.9999");
                        } else {
                            bdv2 = new BigDecimal("1234.56789");
                            bdv2 = bdv2.multiply(BigDecimal.valueOf(sign));
                        }
                        column.appendDecimal(bdv2);
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        BigDecimal bd;
                        if (rowId == 0) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("9999999.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("999999999999999.56789");
                            } else {
                                bd = new BigDecimal("9999999999999999999999999999999999.56789");
                            }
                        } else if (rowId == 1) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("-9999999.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("-999999999999999.56789");
                            } else {
                                bd = new BigDecimal("-9999999999999999999999999999999999.56789");
                            }
                        } else {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("12345.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("123456789012.56789");
                            } else {
                                bd = new BigDecimal("12345678901234567890123.56789");
                            }
                            bd = bd.multiply(BigDecimal.valueOf((long) rowId * sign))
                                    .setScale(pbColumn.getFrac(), RoundingMode.HALF_UP);
                        }
                        column.appendDecimal(bd);
                        break;
                    case CHAR:
                    case VARCHAR:
                        column.appendString(pbColumn.getName() + ":name" + rowId);
                        break;
                    case DATE:
                        Date dt;
                        if (rowId == 0) {
                            dt = Date.valueOf("1900-1-1");
                        } else if (rowId == 1) {
                            dt = Date.valueOf("4096-12-31");
                        } else {
                            dt = Date.valueOf("2023-10-31");
                            dt.setYear(123 + rowId * sign);
                        }
                        column.appendDate(dt);
                        break;
                    case DATETIME:
                        Timestamp ts;
                        if (rowId == 0) {
                            ts = Timestamp.valueOf("1800-11-20 12:34:56");
                        } else if (rowId == 1) {
                            ts = Timestamp.valueOf("4096-11-30 11:22:33");
                        } else {
                            ts = Timestamp.valueOf("2023-12-30 22:33:44");
                            ts.setYear(123 + rowId * sign);
                        }
                        column.appendTimestamp(ts);
                        break;
                    default:
                        throw new IllegalStateException("unsupported column type: " + pbColumn.getType());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("testPartitionTable")
    public void testPartitionTable(String tableName) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        int index = 0;
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                int startRow = index * 400;
                index++;
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleDataWithinDays(tabletSchema, chunk, startRow, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleDataWithinDays(tabletSchema, chunk, startRow + 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(DB_NAME, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNumGroupByDay(DB_NAME, tableName);
        assertEquals(2, outputs.size());
        assertEquals(1000, Integer.valueOf(outputs.get(0).get("num")));
        assertEquals("2024-08-25", outputs.get(0).get("c_date"));

        assertEquals(1000, Integer.valueOf(outputs.get(1).get("num")));
        assertEquals("2024-08-26", outputs.get(1).get("c_date"));
    }


    @ParameterizedTest
    @MethodSource("testListPartitionTable")
    public void testListPartitionTable(String tableName) throws LoadNonSupportException, RequestException {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        String fastSchemaChange = tableSchema.isFastSchemaChange();

        int index = 0;
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                int startRow = index * 400;
                index++;
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    if (fastSchemaChange.equalsIgnoreCase("false")) {
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        String metaContext = restClient.getTabletMeta(metaUrl);
                        options.put("starrocks.format.metaContext", metaContext);
                        options.put("starrocks.format.fastSchemaChange", fastSchemaChange);
                    }
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            -1L,
                            storagePath,
                            options);
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(4096);

                    chunk.reset();
                    fillSampleDataWithinDays(tabletSchema, chunk, startRow, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleDataWithinDays(tabletSchema, chunk, startRow + 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }
            }
        }

        String label = String.format("bypass_write_%s_%s_%s", DB_NAME, tableName, uuid);
        String ak = settings.getS3AccessKey();
        String sk = settings.getS3SecretKey();
        String endpoint = settings.getS3Endpoint();

        loadSegmentData(DB_NAME, label, stageDir, tableName, ak, sk, endpoint);
        boolean res = waitUtilLoadFinished(DB_NAME, label);
        assertTrue(res);

        List<Map<String, String>> outputs = getTableRowNumGroupByCity(DB_NAME, tableName);
        assertEquals(4, outputs.size());
        assertEquals(200, Integer.valueOf(outputs.get(0).get("num")));
        assertEquals("Beijing", outputs.get(0).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(1).get("num")));
        assertEquals("Hangzhou", outputs.get(1).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(2).get("num")));
        assertEquals("Los Angeles", outputs.get(2).get("c_string"));

        assertEquals(200, Integer.valueOf(outputs.get(3).get("num")));
        assertEquals("San Francisco", outputs.get(3).get("c_string"));
    }

    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=4 fill the base value * rowId * sign.
    private static void fillSampleDataWithinDays(TabletSchema.TabletSchemaPB pbSchema, Chunk chunk, int startRowId, int numRows) {

        for (int colIdx = 0; colIdx < pbSchema.getColumnCount(); colIdx++) {
            TabletSchema.ColumnPB pbColumn = pbSchema.getColumn(colIdx);
            Column column = chunk.getColumn(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("rowid".equalsIgnoreCase(pbColumn.getName())) {
                    column.appendInt(rowId);
                    continue;
                }
                DataType dataType = DataType.fromLiteral(pbColumn.getType()).get();

                if (rowId == 2 && (dataType != DataType.DATE && !"c_string".equalsIgnoreCase(pbColumn.getName()))) {
                    column.appendNull();
                    continue;
                }
                int sign = (rowId % 2 == 0) ? -1 : 1;

                switch (dataType) {
                    case BOOLEAN:
                        column.appendBool(rowId % 2 == 0);
                        break;
                    case TINYINT:
                        if (rowId == 0) {
                            column.appendByte(Byte.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendByte(Byte.MIN_VALUE);
                        } else {
                            column.appendByte((byte) (rowId * sign));
                        }
                        break;
                    case SMALLINT:
                        if (rowId == 0) {
                            column.appendShort(Short.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendShort(Short.MIN_VALUE);
                        } else {
                            column.appendShort((short) (rowId * 10 * sign));
                        }
                        break;
                    case INT:
                        if (rowId == 0) {
                            column.appendInt(Integer.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendInt(Integer.MIN_VALUE);
                        } else {
                            column.appendInt(rowId * 100 * sign);
                        }
                        break;
                    case BIGINT:
                        if (rowId == 0) {
                            column.appendLong(Long.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendLong(Long.MIN_VALUE);
                        } else {
                            column.appendLong(rowId * 1000L * sign);
                        }
                        break;
                    case LARGEINT:
                        if (rowId == 0) {
                            column.appendLargeInt(new BigInteger("6693349846790746512344567890123456789"));
                        } else if (rowId == 1) {
                            column.appendLargeInt(new BigInteger("-6693349846790746512344567890123456789"));
                        } else {
                            column.appendLargeInt(BigInteger.valueOf(rowId * 10000L * sign));
                        }
                        break;
                    case FLOAT:
                        column.appendFloat(123.45678901234f * rowId * sign);
                        break;
                    case DOUBLE:
                        column.appendDouble(23456.78901234 * rowId * sign);
                        break;
                    case DECIMAL:
                        // decimal v2 type
                        BigDecimal bdv2;
                        if (rowId == 0) {
                            bdv2 = new BigDecimal("-12345678901234567890123.4567");
                        } else if (rowId == 1) {
                            bdv2 = new BigDecimal("999999999999999999999999.9999");
                        } else {
                            bdv2 = new BigDecimal("1234.56789");
                            bdv2 = bdv2.multiply(BigDecimal.valueOf(sign));
                        }
                        column.appendDecimal(bdv2);
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        BigDecimal bd;
                        if (rowId == 0) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("9999.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("99999999999.56789");
                            } else {
                                bd = new BigDecimal("9999999999999999999999999999.56789");
                            }
                        } else if (rowId == 1) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("-9999.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("-99999999999.56789");
                            } else {
                                bd = new BigDecimal("-9999999999999999999999999999.56789");
                            }
                        } else {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("125.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("12348902.56789");
                            } else {
                                bd = new BigDecimal("1234564567890123.56789");
                            }
                            bd = bd.multiply(BigDecimal.valueOf((long) rowId * sign))
                                    .setScale(pbColumn.getFrac(), RoundingMode.HALF_UP);
                        }
                        column.appendDecimal(bd);
                        break;
                    case CHAR:
                    case VARCHAR:
                        if (rowId % 4 == 0) {
                            column.appendString("Beijing");
                        } else if (rowId % 4 == 1) {
                            column.appendString("Hangzhou");
                        } else if (rowId % 4 == 2) {
                            column.appendString("Los Angeles");
                        } else if (rowId % 4 == 3) {
                            column.appendString("San Francisco");
                        }
                        break;
                    case DATE:
                        Date dt;
                        if (sign == 1) {
                            dt = Date.valueOf("2024-08-25");
                        } else {
                            dt = Date.valueOf("2024-08-26");
                        }
                        column.appendDate(dt);
                        break;
                    case DATETIME:
                        Timestamp ts;
                        if (rowId == 0) {
                            ts = Timestamp.valueOf("1800-11-20 12:34:56");
                        } else if (rowId == 1) {
                            ts = Timestamp.valueOf("4096-11-30 11:22:33");
                        } else {
                            ts = Timestamp.valueOf("2023-12-30 22:33:44");
                            ts.setYear(123 + rowId * sign);
                        }
                        column.appendTimestamp(ts);
                        break;
                    default:
                        throw new IllegalStateException("unsupported column type: " + pbColumn.getType());
                }
            }
        }
    }

    public List<Map<String, String>> getTableRowNum(String db, String table) {
        String queryStmt = String.format("select count(*) as num from `%s`.`%s`;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }

    public List<Map<String, String>> getTableRowNumNotNUll(String db, String table) {
        String queryStmt = String.format("select count(*) as num from `%s`.`%s` where c_int is not null;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }


    public List<Map<String, String>> getTableRowNumGroupByDay(String db, String table) {
        String queryStmt = String.format(" select c_date,count(*) AS num from  `%s`.`%s` " +
                "group by c_date order by c_date;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }
    public List<Map<String, String>> getTableRowNumGroupByCity(String db, String table) {
        String queryStmt = String.format(" select c_string,count(*) AS num from  `%s`.`%s` " +
                "group by c_string order by c_string;", db, table);
        return executeSqlWithReturn(queryStmt, new ArrayList<>());
    }

    public void loadSegmentData(String db, String label, String stagingPath, String table,
                                String ak, String sk, String endpoint) {
        String loadSegment = String.format("LOAD LABEL %s.`%s` " +
                "( " +
                " DATA INFILE(\"%s\") " +
                " INTO TABLE %s " +
                " FORMAT AS \"starrocks\" " +
                ") WITH BROKER (" +
                "\"aws.s3.use_instance_profile\" = \"false\"," +
                "\"aws.s3.access_key\" = \"%s\"," +
                "\"aws.s3.secret_key\" = \"%s\"," +
                "\"aws.s3.endpoint\" = \"%s\"," +
                "\"aws.s3.enable_ssl\" = \"false\"" +
                ");", db, label, stagingPath, table, ak, sk, endpoint);
        executeSql(loadSegment);
    }

    public List<Map<String, String>> getSegmentLoadState(String db, String label) {
        String loadSegment = String.format("SHOW LOAD FROM %s WHERE LABEL = \"%s\" ORDER BY CreateTime desc limit 1;",
                db, label);
        return executeSqlWithReturn(loadSegment, new ArrayList<>());
    }

    public boolean waitUtilLoadFinished(String db, String label) {
        try {
            Thread.sleep(2000);
            String state;
            long timeout = 60000;
            long starTime = System.currentTimeMillis() / 1000;
            do {
                List<Map<String, String>> loads = getSegmentLoadState(db, label);
                if (loads.isEmpty()) {
                    return false;
                }
                // loads only have one row
                for (Map<String, String> l : loads) {
                    state = l.get("State");
                    if (state.equalsIgnoreCase("CANCELLED")) {
                        System.out.println("Load had failed with error: " + l.get("ErrorMsg"));
                        return false;
                    } else if (state.equalsIgnoreCase("Finished")) {
                        return true;
                    } else {
                        System.out.println("Load had not finished, try another loop with state = " + state);
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
