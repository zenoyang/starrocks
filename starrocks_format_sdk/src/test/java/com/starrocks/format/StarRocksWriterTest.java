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

import com.starrocks.format.rest.TransactionResult;
import com.starrocks.format.rest.model.TablePartition;
import com.starrocks.format.rest.model.TabletCommitInfo;
import com.starrocks.proto.LakeTypes;
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
import java.io.FileOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;


public class StarRocksWriterTest extends BaseFormatTest {

    @BeforeAll
    public static void init() throws Exception {
        BaseFormatTest.init();
    }

    private static Stream<Arguments> transactionWriteTables() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate"),
                Arguments.of("tb_all_primitivetype_write_unique"),
                Arguments.of("tb_all_primitivetype_write_aggregate"),
                Arguments.of("tb_all_primitivetype_write_primary")
        );
    }

    @ParameterizedTest
    @MethodSource("transactionWriteTables")
    public void testTransactionWrite(String tableName) throws Exception {
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

            for (TablePartition.Tablet tablet : tablets) {
                Long tabletId = tablet.getId();
                Long backendId = tablet.getPrimaryComputeNodeId();

                try {
                    StarRocksWriter writer = new StarRocksWriter(tabletId,
                            tabletSchema,
                            beginTxnResult.getTxnId(),
                            partition.getStoragePath(),
                            settings.toMap());
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

                committedTablets.add(new TabletCommitInfo(tabletId, backendId));
            }
        }

        TransactionResult prepareTxnResult = restClient.prepareTransaction(
                DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), committedTablets, null);
        assertTrue(prepareTxnResult.isOk());

        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());
    }

    @Test
    public void testWriteLocalUseChunk(@TempDir Path tempDir) throws Exception {
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.BOOLEAN, 4),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DATE, 4),
                new ColumnType(DataType.DATETIME, 8),
                new ColumnType(DataType.DECIMAL32, 8),
                new ColumnType(DataType.DECIMAL64, 16),
                new ColumnType(DataType.VARCHAR, 32 + Integer.SIZE)};

        int colId = 0;
        for (ColumnType columnType : columnTypes) {
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
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

        StarRocksWriter writer = new StarRocksWriter(tabletId,
                schema,
                txnId,
                tabletRootPath,
                new HashMap<>(0));
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
    }

    @Test
    public void testWriteStringExtendLength(@TempDir Path tempDir) throws Exception {
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] columnTypes = new ColumnType[] {
                new ColumnType(DataType.CHAR, 9),
                new ColumnType(DataType.VARCHAR, 17)};

        int colId = 0;
        for (ColumnType columnType : columnTypes) {
            schemaBuilder.clearColumn();
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName("c_" + columnType.getDataType())
                    .setUniqueId(0)
                    .setIsKey(true)
                    .setIsNullable(true)
                    .setType(columnType.getDataType().getLiteral())
                    .setLength(columnType.getLength())
                    .setIndexLength(columnType.getLength())
                    .setAggregation("none");
            schemaBuilder.addColumn(columnBuilder.build());

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

            String tabletRootPath = tempDir.toAbsolutePath().toString() + "/" + columnType.getDataType();
            File dir = new File(tabletRootPath + "/data");
            assertTrue(dir.mkdirs());
            dir = new File(tabletRootPath + "/log");
            assertTrue(dir.mkdirs());
            dir = new File(tabletRootPath + "/meta");
            assertTrue(dir.mkdirs());

            LakeTypes.TabletMetadataPB metadata = LakeTypes.TabletMetadataPB.newBuilder()
                    .setSchema(schema).build();
            FileOutputStream out = new FileOutputStream(new File(dir, String.format("%016x_%016x.meta", tabletId, 1)));
            metadata.writeTo(out);
            out.close();

            StarRocksWriter writer = new StarRocksWriter(tabletId,
                    schema,
                    txnId,
                    tabletRootPath,
                    new HashMap<>(0));
            writer.open();

            // write use chunk interface
            Chunk chunk = writer.newChunk(5);
            Column column = chunk.getColumn(0);

            // normal append
            String value = generateFixedLengthString(columnType.getLength());
            column.appendString(value);

            // abnormal append
            try {
                value = generateFixedLengthString(columnType.getLength() + 1 );
                column.appendString(value);
                fail("append long string should failed.");
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("string length"), e.getMessage());
            }

            chunk.release();
            writer.release();
        }

    }

    @Test
    public void testUnsupportedColumnType(@TempDir Path tempDir) throws Exception {
        TabletSchemaPB.Builder schemaBuilder = TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] types = new ColumnType[] {
                new ColumnType(DataType.INT, 4),
                new ColumnType(DataType.BOOLEAN, 4),
                new ColumnType(DataType.TINYINT, 1),
                new ColumnType(DataType.SMALLINT, 2),
                new ColumnType(DataType.BIGINT, 8),
                new ColumnType(DataType.LARGEINT, 16),
                new ColumnType(DataType.FLOAT, 4),
                new ColumnType(DataType.DOUBLE, 8),
                new ColumnType(DataType.DATE, 4),
                new ColumnType(DataType.DATETIME, 8),
                new ColumnType(DataType.DECIMAL32, 8),
                new ColumnType(DataType.DECIMAL64, 16),
                new ColumnType(DataType.VARCHAR, 32 + Integer.SIZE),
                new ColumnType(DataType.MAP, 16),
                new ColumnType(DataType.ARRAY, 16),
                new ColumnType(DataType.STRUCT, 16),
                new ColumnType(DataType.JSON, 16)};

        int colId = 0;
        for (ColumnType type : types) {
            ColumnPB.Builder columnBuilder = ColumnPB.newBuilder()
                    .setName("c_" + type.getDataType())
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(false)
                    .setType(type.getDataType().getLiteral())
                    .setLength(type.getLength())
                    .setIndexLength(type.getLength())
                    .setAggregation("none");
            if (type.getDataType().getLiteral().startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(3);
            } else if (type.getDataType().getLiteral().startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(4);
            } else if (type.getDataType().getLiteral().startsWith("DECIMAL128")) {
                columnBuilder.setPrecision(29);
                columnBuilder.setFrac(5);
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
        long txnId = 4L;

        String tabletRootPath = tempDir.toAbsolutePath().toString();
        File dir = new File(tabletRootPath + "/data");
        assertTrue(dir.mkdirs());
        dir = new File(tabletRootPath + "/log");
        assertTrue(dir.mkdirs());

        try {
            StarRocksWriter writer = new StarRocksWriter(
                    tabletId,
                    schema,
                    txnId,
                    tabletRootPath,
                    new HashMap<>(0));
            writer.open();
            fail();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(e.getMessage().contains("Unsupported column type"));
        }
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

    public static String generateFixedLengthString(int length) {
        StringBuilder stringBuilder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            stringBuilder.append('a');
        }
        return stringBuilder.toString();
    }
}
