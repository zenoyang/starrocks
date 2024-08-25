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
//                Arguments.of("tb_fast_schema_change_table")
                Arguments.of("tb_no_fast_schema_change_table")
        );
    }

    @ParameterizedTest
    @MethodSource("testAllPrimitiveType")
    public void testAllPrimitiveType(String tableName) throws Exception {
        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        if (!Validator.assignUniqColumnId(tableSchema)) {
            throw new Exception("Unable to assign unique column id.");
        }
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    // http://10.37.55.121:8040/api/meta/header/15143
                    String metaUrl = tablet.getMetaUrls().get(0);
                    options.put("starrocks.format.tablet_url", metaUrl);
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
            if (!Validator.assignUniqColumnId(tableSchema)) {
                throw new Exception("Unable to assign unique column id.");
            }
            TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

            List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME,
                    tableName, false);
            long tableId = tableSchema.getId();
            long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

            assertFalse(partitions.isEmpty());
            for (TablePartition partition : partitions) {
                List<TablePartition.Tablet> tablets = partition.getTablets();
                assertFalse(tablets.isEmpty());

                for (TablePartition.Tablet tablet : tablets) {
                    Long tabletId = tablet.getId();
                    try {
                        String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                                + indexId + "/" + tabletId;
                        Map<String, String> options = settings.toMap();
                        // http://10.37.55.121:8040/api/meta/header/15143
                        String metaUrl = tablet.getMetaUrls().get(0);
                        options.put("starrocks.format.tablet_url", metaUrl);
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

        String uuid = RandomStringUtils.randomAlphabetic(8);
        String stageDir = "s3a://bucket1/.staging_ut/" + uuid + "/";
        TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName);
        Validator.validateSegmentLoadExport(tableSchema);
        if (!Validator.assignUniqColumnId(tableSchema)) {
            throw new Exception("Unable to assign unique column id.");
        }
        TabletSchema.TabletSchemaPB tabletSchema = toPbTabletSchema(tableSchema);

        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        long tableId = tableSchema.getId();
        long indexId = tableSchema.getIndexMetas().get(0).getIndexId();

        assertFalse(partitions.isEmpty());
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                try {
                    String storagePath = stageDir + "/" + tableId + "/" + partition.getId() + "/"
                            + indexId + "/" + tabletId;
                    Map<String, String> options = settings.toMap();
                    // http://10.37.55.121:8040/api/meta/header/15143
                    String metaUrl = tablet.getMetaUrls().get(0);
                    String metaContext = restClient.getTabletMeta(metaUrl);
                    options.put("starrocks.format.metaContext", metaContext);
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

    public List<Map<String, String>> getTableRowNum(String db, String table) {
        String queryStmt = String.format("select count(*) as num from `%s`.`%s`;", db, table);
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
