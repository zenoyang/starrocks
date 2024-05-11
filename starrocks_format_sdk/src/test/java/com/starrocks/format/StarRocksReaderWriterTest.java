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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.starrocks.format.rest.TransactionResult;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TabletCommitInfo;
import com.starrocks.proto.LakeTypes;
import com.starrocks.proto.TabletSchema;
import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.KeysType;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;
import com.starrocks.proto.Types.CompressionTypePB;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;


public class StarRocksReaderWriterTest extends BaseFormatTest {

    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }

    private static Stream<Arguments> testReadAfterWrite() {
        return Stream.of(
                Arguments.of("tb_json_two_key_primary"),
                Arguments.of("tb_json_two_key_unique")
        );
    }

    @ParameterizedTest
    @MethodSource("testReadAfterWrite")
    public void testReadAfterWrite(String tableName) throws Exception {
        String label = String.format("bypass_write_%s_%s_%s",
                DB_NAME, tableName, RandomStringUtils.randomAlphabetic(8));
        TabletSchemaPB tabletSchema = toPbTabletSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        List<TablePartition> partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        assertFalse(partitions.isEmpty());

        // begin transaction
        TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, DB_NAME, tableName, label);
        assertTrue(beginTxnResult.isOk());

        List<TabletCommitInfo> committedTablets = new ArrayList<>();
        for (TablePartition partition : partitions) {
            List<TablePartition.Tablet> tablets = partition.getTablets();
            assertFalse(tablets.isEmpty());

            int tabletIndex = 0;
            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();
                int startId = tabletIndex * 1000;
                tabletIndex++;

                try {
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            beginTxnResult.getTxnId(),
                            partition.getStoragePath(),
                            settings.toMap());
                    writer.open();
                    // write use chunk interface
                    Chunk chunk = writer.newChunk(3);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, startId, 3);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(tabletSchema, chunk, startId + 200, 3);
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

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());

        // read all data test
        int expectedNumRows = 18;
        // read chunk
        long totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                StarRocksReader reader = new StarRocksReader(
                        tabletId, version, tabletSchema, tabletSchema, partition.getStoragePath(), settings.toMap());
                reader.open();

                long numRows;
                do {
                    Chunk chunk = reader.getNext();
                    numRows = chunk.numRow();

                    checkValue(tabletSchema, chunk, numRows);
                    chunk.release();

                    totalRows += numRows;
                } while (numRows > 0);

                // should be empty chunk
                Chunk chunk = reader.getNext();
                assertEquals(0, chunk.numRow());
                chunk.release();

                reader.close();
                reader.release();
            }
        }

        assertEquals(expectedNumRows, totalRows);

        // test with filter
        String requiredColumns =  "rowid,c_varchar,c_json";
        String outputColumns =      "rowid,c_varchar";
        String sql=   "select rowId, c_varchar from demo." + tableName + " where cast((c_json->'rowid') as int) % 2 = 0";
        TabletSchemaPB tabletAllSchema = toPbTabletSchema(restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, tableName));
        String queryPlan = restClient.getQueryPlan(DB_NAME, tableName, sql).getOpaquedQueryPlan();
        Set<String> requiredColumnName = new HashSet<>(Arrays.asList(requiredColumns.split(",")));
        Set<String> outputColumnName = new HashSet<>(Arrays.asList(outputColumns.split(",")));

        TabletSchemaPB.Builder builder = tabletAllSchema.toBuilder().clearColumn();
        List<TabletSchema.ColumnPB> pbColumns = tabletAllSchema.getColumnList().stream()
                .filter(col -> requiredColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());

        builder.addAllColumn(pbColumns);
        TabletSchemaPB requiredSchema = builder.build();

        pbColumns = tabletAllSchema.getColumnList().stream()
                .filter(col -> outputColumnName.contains(col.getName().toLowerCase()))
                .collect(Collectors.toList());

        TabletSchemaPB outputSchema = builder.clearColumn().addAllColumn(pbColumns).build();

        // read chunk
        int expectedTotalRows = 11;
        totalRows = 0;
        partitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, tableName, false);
        for (TablePartition partition : partitions) {
            long version = partition.getVisibleVersion();
            for (TablePartition.Tablet tablet : partition.getTablets()) {
                Long tabletId = tablet.getId();

                try {
                    Map<String, String> options = settings.toMap();
                    options.put(STARROCKS_FORMAT_QUERY_PLAN, queryPlan);
                    // read table
                    StarRocksReader reader = new StarRocksReader(
                            tabletId, version, requiredSchema, outputSchema, partition.getStoragePath(), options);
                    reader.open();

                    long numRows;
                    do {
                        Chunk chunk = reader.getNext();
                        numRows = chunk.numRow();

                        checkValue(outputSchema, chunk, numRows);
                        chunk.release();

                        totalRows += numRows;
                    } while (numRows > 0);

                    // should be empty chunk
                    Chunk chunk = reader.getNext();
                    assertEquals(0, chunk.numRow());
                    chunk.release();

                    reader.close();
                    reader.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail();
                }

            }
        }

        assertEquals(expectedTotalRows, totalRows);

    }


    @Test
    public void testWriteLongJson(@TempDir Path tempDir) throws Exception {
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        // add two key column
        ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                .setName("rowId")
                .setUniqueId(0)
                .setIsKey(true)
                .setIsNullable(false)
                .setType(DataType.INT.getLiteral())
                .setLength(4)
                .setIndexLength(4)
                .setAggregation("none");
        schemaBuilder.addColumn(columnBuilder.build());
        columnBuilder = ColumnPB.newBuilder()
                .setName("rowId2")
                .setUniqueId(1)
                .setIsKey(true)
                .setIsNullable(false)
                .setType(DataType.INT.getLiteral())
                .setLength(4)
                .setIndexLength(4)
                .setAggregation("none");
        schemaBuilder.addColumn(columnBuilder.build());


        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.JSON, 4)};
        int colId = 2;
        for (ColumnType columnType : columnTypes) {
            columnBuilder = ColumnPB.newBuilder()
                    .setName("c_" + columnType.getDataType())
                    .setUniqueId(colId)
                    .setIsKey(false)
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
        TabletSchemaPB schema = schemaBuilder
                .setNextColumnUniqueId(colId)
                // sort key index alway the key column index
                .addSortKeyIdxes(0)
                // short key size is less than sort keys
                .setNumShortKeyColumns(1)
                .setNumRowsPerRowBlock(1024)
                .build();

        long tabletId = 100L;
        long txnId = 4;
        long version = 1;

        String tabletRootPath = tempDir.toAbsolutePath().toString();

        setupTabletMeta(tabletRootPath, schema, tabletId, version);

        StarRocksWriter writer = new StarRocksWriter(tabletId,
                schema,
                txnId,
                tabletRootPath,
                new HashMap<>(0));
        writer.open();

        // write use chunk interface
        Chunk chunk = writer.newChunk(5);

        Column jsonColumn = chunk.getColumn(2);
        // normal append
        String value = generateFixedLengthString(100);
        jsonColumn.appendString(value);

        // abnormal append
        try {
            value = generateFixedLengthString(16 * 1024 * 1024 + 1);
            jsonColumn.appendString(value);
            fail("append long string should failed.");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("string exceed maximum length"), e.getMessage());
        }
        chunk.release();
        writer.close();
        writer.release();
    }

    private static void setupTabletMeta(String tabletRootPath, TabletSchemaPB schema, long tabletId, long version) throws IOException {
        File dir = new File(tabletRootPath + "/data");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/log");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/meta");
        assertTrue(dir.mkdirs());

        LakeTypes.TabletMetadataPB metadata = LakeTypes.TabletMetadataPB.newBuilder()
                .setSchema(schema).build();

        FileOutputStream out = new FileOutputStream(new File(dir, String.format("%016x_%016x.meta", tabletId, version)));
        metadata.writeTo(out);
        out.close();

        out = new FileOutputStream(new File(dir, String.format("%016x_%016x.meta", tabletId, version + 1)));
        metadata.writeTo(out);
        out.close();
    }

    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=4 fill the base value * rowId * sign.
    private static void fillSampleData(TabletSchemaPB pbSchema, Chunk chunk, int startRowId, int numRows) {

        for (int colIdx = 0; colIdx < pbSchema.getColumnCount(); colIdx++) {
            ColumnPB pbColumn = pbSchema.getColumn(colIdx);
            Column column = chunk.getColumn(colIdx);
            for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if ("rowid".equalsIgnoreCase(pbColumn.getName())) {
                    column.appendInt(rowId);
                    continue;
                }
                if ("rowid2".equalsIgnoreCase(pbColumn.getName())) {
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
                    case JSON:
                        Gson gson = new Gson();
                        Map<String, Object> jsonMap = new HashMap<>();
                        jsonMap.put("rowid", rowId);
                        boolean boolVal = rowId % 2 == 0;
                        jsonMap.put("bool", boolVal);
                        int intVal = 0;
                        if (rowId == 0) {
                            intVal = Integer.MAX_VALUE;
                        } else if (rowId == 1) {
                            intVal = Integer.MIN_VALUE;
                        } else {
                            intVal = rowId * 100 * sign;
                        }
                        jsonMap.put("int", intVal);
                        jsonMap.put("varchar", pbColumn.getName() + ":name" + rowId);
                        String json = gson.toJson(jsonMap);
                        column.appendString(json);
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


    private static void checkValue(TabletSchemaPB schema, Chunk chunk, long numRows) {
        for (int rowIdx = 0; rowIdx < numRows; rowIdx++) {
            int rowId = 0;
            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                ColumnPB pbColumn = schema.getColumn(colIdx);
                Column column = chunk.getColumn(colIdx);
                if ("rowid".equalsIgnoreCase(pbColumn.getName())) {
                    rowId = column.getInt(rowIdx);
                    break;
                }
            }

            for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
                ColumnPB pbColumn = schema.getColumn(colIdx);
                Column column = chunk.getColumn(colIdx);
                if ("rowid".equalsIgnoreCase(pbColumn.getName())) {
                    assertEquals(rowId, column.getInt(rowIdx));
                    continue;
                }
                if ("rowid2".equalsIgnoreCase(pbColumn.getName())) {
//                    assertEquals(rowId, column.getInt(rowIdx));
                    continue;
                }
                if (rowId == 2) {
                    assertTrue(
                            column.isNullAt(rowIdx),
                            "column " + pbColumn.getName() + " row:" + rowId + " should be null."
                    );
                    continue;
                }
                assertFalse(
                        column.isNullAt(rowIdx),
                        "column " + pbColumn.getName() + " row:" + rowId + " should not be null."
                );
                int sign = (rowId % 2 == 0) ? -1 : 1;

                DataType dataType = DataType.fromLiteral(pbColumn.getType()).get();
                switch (dataType) {
                    case BOOLEAN:
                        if (rowId % 2 == 0) {
                            assertTrue(column.getBoolean(rowIdx));
                        } else {
                            assertFalse(column.getBoolean(rowIdx));
                        }
                        break;
                    case TINYINT:
                        if (rowId == 0) {
                            assertEquals(Byte.MAX_VALUE, column.getByte(rowIdx));
                        } else if (rowId == 1) {
                            assertEquals(Byte.MIN_VALUE, column.getByte(rowIdx));
                        } else {
                            assertEquals(rowId * sign, column.getByte(rowIdx));
                        }
                        break;
                    case SMALLINT:
                        if (rowId == 0) {
                            assertEquals(Short.MAX_VALUE, column.getShort(rowIdx));
                        } else if (rowId == 1) {
                            assertEquals(Short.MIN_VALUE, column.getShort(rowIdx));
                        } else {
                            assertEquals(rowId * 10 * sign, column.getShort(rowIdx));
                        }
                        break;
                    case INT:
                        int intValue;
                        if (rowId == 0) {
                            intValue = Integer.MAX_VALUE;
                        } else if (rowId == 1) {
                            intValue = Integer.MIN_VALUE;
                        } else {
                            intValue = rowId * 100 * sign;
                        }
                        assertEquals(intValue, column.getInt(rowIdx));
                        break;
                    case BIGINT:
                        long longValue;
                        if (rowId == 0) {
                            longValue = Long.MAX_VALUE;
                        } else if (rowId == 1) {
                            longValue = Long.MIN_VALUE;
                        } else {
                            longValue = rowId * 1000L * sign;
                        }
                        assertEquals(longValue, column.getLong(rowIdx));
                        break;
                    case LARGEINT:
                        BigInteger biv;
                        if (rowId == 0) {
                            biv = new BigInteger("6693349846790746512344567890123456789");
                        } else if (rowId == 1) {
                            biv = new BigInteger("-6693349846790746512344567890123456789");
                        } else {
                            biv = BigInteger.valueOf(rowId * 10000L * sign);
                        }
                        assertEquals(biv, column.getLargeInt(rowIdx));
                        break;
                    case FLOAT:
                        assertTrue(Math.abs(123.45678901234f * rowId * sign - column.getFloat(rowIdx)) < 0.0001);
                        break;
                    case DOUBLE:
                        assertTrue(Math.abs(23456.78901234 * rowId * sign - column.getDouble(rowIdx)) < 0.0001);
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
                        assertEquals(bdv2, column.getDecimal(rowIdx));
                        break;
                    case DECIMAL32:
                    case DECIMAL64:
                    case DECIMAL128:
                        BigDecimal bd;
                        if (rowId == 0) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("9999999.57");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("999999999999999.568");
                            } else {
                                bd = new BigDecimal("9999999999999999999999999999999999.5679");
                            }
                        } else if (rowId == 1) {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("-9999999.57");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("-999999999999999.568");
                            } else {
                                bd = new BigDecimal("-9999999999999999999999999999999999.5679");
                            }
                        } else {
                            if (pbColumn.getPrecision() <= 9) {
                                bd = new BigDecimal("12345.5678");
                            } else if (pbColumn.getPrecision() <= 18) {
                                bd = new BigDecimal("123456789012.56789");
                            } else {
                                bd = new BigDecimal("12345678901234567890123.56789");
                            }
                            bd = bd.multiply(BigDecimal.valueOf(rowId * sign)).setScale(pbColumn.getFrac(), RoundingMode.HALF_UP);
                        }
                        assertEquals(bd, column.getDecimal(rowIdx));
                        break;
                    case CHAR:
                    case VARCHAR:
                        assertEquals(pbColumn.getName() + ":name" + rowId, column.getString(rowIdx));
                        break;
                    case JSON:
                        String rowStr = column.getString(rowIdx);
                        Gson gson = new Gson();
                        Map<String, Object> resultMap = gson.fromJson(rowStr, new TypeToken<Map<String, Object>>(){}.getType());
                        assertEquals(rowId, (int)Math.round((Double) resultMap.get("rowid")));
                        boolean boolVal = rowId % 2 == 0;
                        assertEquals(boolVal, resultMap.get("bool"));
                        int intVal = 0;
                        if (rowId == 0) {
                            intVal = Integer.MAX_VALUE;
                        } else if (rowId == 1) {
                            intVal = Integer.MIN_VALUE;
                        } else {
                            intVal = rowId * 100 * sign;
                        }
                        assertEquals(intVal, (int)Math.round((Double) resultMap.get("int")));
                        assertEquals(pbColumn.getName() + ":name" + rowId, resultMap.get("varchar"));
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
                        assertEquals(dt, column.getDate(rowIdx, TimeZone.getDefault()));
                        break;
                    case DATETIME:
                        Timestamp ts;
                        if (rowId == 0) {
                            ts = Timestamp.valueOf("1800-11-20 12:40:39");
                        } else if (rowId == 1) {
                            ts = Timestamp.valueOf("4096-11-30 11:22:33");
                        } else {
                            ts = Timestamp.valueOf("2023-12-30 22:33:44");
                            ts.setYear(123 + rowId * sign);
                        }
                        assertEquals(ts, column.getTimestamp(rowIdx, TimeZone.getDefault()));
                        break;
                    default:
                        throw new IllegalStateException("unsupported column type: " + pbColumn.getType());
                }
            }
        }
    }

    public static String generateFixedLengthString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append('a');
        }
        return stringBuilder.toString();
    }
}
