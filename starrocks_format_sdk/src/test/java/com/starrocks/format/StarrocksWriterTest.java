package com.starrocks.format;

import com.starrocks.format.rest.RestService;
import com.starrocks.format.rest.models.Schema;
import com.starrocks.format.rest.models.TabletCommitInfo;
import com.starrocks.proto.TabletSchema;
import com.starrocks.proto.Types.CompressionTypePB;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.*;
import java.util.stream.Stream;

import static com.starrocks.format.rest.RestService.DB;
import static com.starrocks.format.rest.RestService.LABEL;
import static com.starrocks.format.rest.RestService.TABLE;
import static com.starrocks.format.rest.RestService.TXN_ID;


public class StarrocksWriterTest {

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    static Stream<Arguments> testTransactionWrite() {
        return Stream.of(
                Arguments.of("tb_all_primitivetype_write_duplicate"),
                Arguments.of("tb_all_primitivetype_write_unique"),
                Arguments.of("tb_all_primitivetype_write_aggregate"),
                Arguments.of("tb_all_primitivetype_write_primary")
        );
    }

    @ParameterizedTest
    @MethodSource
    public void testTransactionWrite(String tableName) {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(DB, "demo");
        cfg.put(TABLE, tableName);
        cfg.put(LABEL, String.format("bypass_write_%s_%s_%s",
                cfg.get(DB), cfg.get(TABLE), RandomStringUtils.randomAlphabetic(8)));
        // begin transaction
        RestService.TransactionContext transactionContext = RestService.beginTransaction(cfg);
        Assertions.assertTrue(transactionContext.isOk());
        cfg.put(TXN_ID, String.valueOf(transactionContext.getTxnId()));
        cfg.put(LABEL, String.valueOf(transactionContext.getLabel()));


        // get schema and shard info
        Schema srSchema = RestService.getShardInfo(cfg);
        TabletSchema.TabletSchemaPB pbSchema = srSchema.getEtlTable().convert();

        // writer chunk
        List<TabletCommitInfo> tabletCommitInfoList = new ArrayList<>();
        srSchema.getEtlTable().getPartitionInfo().getPartitions().forEach(partition -> {
            for (int i = 0; i < partition.getTabletIds().size(); i++) {
                long tabletId = partition.getTabletIds().get(i);
                long backendId = partition.getBackendIds().get(i);

                Map<String, String> options = new HashMap<>();
                options.put("fs.s3a.endpoint", "http://127.0.0.1:9000");
                options.put("fs.s3a.endpoint.region", "cn-beijing");
                options.put("fs.s3a.connection.ssl.enabled", "false");
                options.put("fs.s3a.path.style.access", "false");
                options.put("fs.s3a.access.key", "minio_access_key");
                options.put("fs.s3a.secret.key", "minio_secret_key");

                StarrocksWriter writer = new StarrocksWriter(tabletId,
                        pbSchema,
                        transactionContext.getTxnId(),
                        partition.getStoragePath(),
                        options);
                writer.open();
                // write use chunk interface
                Chunk chunk = writer.newChunk(4096);

                try {
                    chunk.reset();
                    fillSampleData(pbSchema, chunk, 0, 200);
                    writer.write(chunk);

                    chunk.reset();
                    fillSampleData(pbSchema, chunk, 200, 200);
                    writer.write(chunk);

                    chunk.release();
                    writer.flush();
                    writer.finish();
                    writer.close();
                    writer.release();
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail();
                }

                tabletCommitInfoList.add(new TabletCommitInfo(tabletId, backendId));
            }
        });
        RestService.prepareTransaction(cfg, tabletCommitInfoList);
        RestService.commitTransaction(cfg);
    }

    @Test
    public void testWriteLocalUseChunk(@TempDir Path tempDir) {

        TabletSchema.TabletSchemaPB.Builder schemaBuilder = TabletSchema.TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(TabletSchema.KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] types = new ColumnType[] {
                new ColumnType("INT", 4),
                new ColumnType("BOOLEAN", 4),
                new ColumnType("TINYINT", 1),
                new ColumnType("SMALLINT", 2),
                new ColumnType("BIGINT", 8),
                new ColumnType("LARGEINT", 16),
                new ColumnType("FLOAT", 4),
                new ColumnType("DOUBLE", 8),
                new ColumnType("DATE", 4),
                new ColumnType("DATETIME", 8),
                new ColumnType("DECIMAL32", 8),
                new ColumnType("DECIMAL64", 16),
                new ColumnType("VARCHAR", 32 + Integer.SIZE)};

        int colId = 0;
        for (ColumnType type : types) {
            TabletSchema.ColumnPB.Builder columnBuilder = TabletSchema.ColumnPB.newBuilder();
            columnBuilder
                    .setName("c_" + type.typeName)
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(true)
                    .setType(type.typeName)
                    .setLength(type.length)
                    .setIndexLength(type.length)
                    .setAggregation("none");
            if (type.typeName.startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(2);
            } else if (type.typeName.startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(3);
            } else if (type.typeName.startsWith("DECIMAL128")) {
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

        long tabletId = 100;
        long txnId = 4;
        Map<String, String> options = new HashMap<>();

        String tabletRootPath = tempDir.toAbsolutePath().toString();
        File dir = new File(tabletRootPath + "/data");
        dir.mkdirs();
        dir = new File(tabletRootPath + "/log");
        dir.mkdirs();
        StarrocksWriter writer = new StarrocksWriter(tabletId,
                schema,
                txnId,
                tabletRootPath,
                options);
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

    // when rowId is 0, fill the max value,
    // 1 fill the min value,
    // 2 fill null,
    // >=4 fill the base value * rowId * sign.
    private void fillSampleData(TabletSchema.TabletSchemaPB schema, Chunk chunk, int startRowId, int num_rows) {

        for (int colIdx = 0; colIdx < schema.getColumnCount(); colIdx++) {
            TabletSchema.ColumnPB columnPB = schema.getColumn(colIdx);
            Column column = chunk.getColumn(colIdx);
            for (int rowIdx = 0; rowIdx < num_rows; rowIdx++) {
                int rowId = startRowId + rowIdx;
                if (columnPB.getName().equalsIgnoreCase("rowid")) {
                    column.appendInt(rowId);
                    continue;
                }
                if (rowId ==  2) {
                    column.appendNull();
                    continue;
                }
                int sign = (rowId % 2 == 0) ? -1 : 1;
                switch (columnPB.getType()) {
                    case "BOOLEAN":
                        if (rowId % 2 == 0) {
                            column.appendBool(true);
                        } else {
                            column.appendBool(false);
                        }
                        break;
                    case "TINYINT":
                        if (rowId == 0) {
                            column.appendByte(Byte.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendByte(Byte.MIN_VALUE);
                        } else {
                            column.appendByte((byte) (rowId * sign));
                        }
                        break;
                    case "SMALLINT":
                        if (rowId == 0) {
                            column.appendShort(Short.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendShort(Short.MIN_VALUE);
                        } else {
                            column.appendShort((short) (rowId * 10 * sign));
                        }
                        break;
                    case "INT":
                        if (rowId == 0) {
                            column.appendInt(Integer.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendInt(Integer.MIN_VALUE);
                        } else {
                            column.appendInt(rowId * 100 * sign);
                        }
                        break;
                    case "BIGINT":
                        if (rowId == 0) {
                            column.appendLong(Long.MAX_VALUE);
                        } else if (rowId == 1) {
                            column.appendLong(Long.MIN_VALUE);
                        } else {
                            column.appendLong(rowId * 1000L * sign);
                        }
                        break;
                    case "LARGEINT":
                        if (rowId == 0) {
                            column.appendLargeInt(new BigInteger("6693349846790746512344567890123456789"));
                        } else if (rowId == 1) {
                            column.appendLargeInt(new BigInteger("-6693349846790746512344567890123456789"));
                        } else {
                            column.appendLargeInt(BigInteger.valueOf(rowId * 10000L * sign));
                        }
                        break;
                    case "FLOAT":
                        column.appendFloat(123.45678901234f * rowId * sign);
                        break;
                    case "DOUBLE":
                        column.appendDouble(23456.78901234 * rowId * sign);
                        break;
                    case "DATE":
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
                    case "DATETIME":
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
                    case "DECIMAL":
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
                    case "DECIMAL32":
                    case "DECIMAL64":
                    case "DECIMAL128":
                        BigDecimal bd;
                        if (rowId == 0) {
                            if (columnPB.getPrecision() <= 9) {
                                bd = new BigDecimal("9999999.5678");
                            } else if (columnPB.getPrecision() <= 18) {
                                bd = new BigDecimal("999999999999999.56789");
                            } else {
                                bd = new BigDecimal("9999999999999999999999999999999999.56789");
                            }
                        } else if (rowId == 1) {
                            if (columnPB.getPrecision() <= 9) {
                                bd = new BigDecimal("-9999999.5678");
                            } else if (columnPB.getPrecision() <= 18) {
                                bd = new BigDecimal("-999999999999999.56789");
                            } else {
                                bd = new BigDecimal("-9999999999999999999999999999999999.56789");
                            }
                        } else {
                            if (columnPB.getPrecision() <= 9) {
                                bd = new BigDecimal("12345.5678");
                            } else if (columnPB.getPrecision() <= 18) {
                                bd = new BigDecimal("123456789012.56789");
                            } else {
                                bd = new BigDecimal("12345678901234567890123.56789");
                            }
                            bd = bd.multiply(BigDecimal.valueOf(rowId * sign)).setScale(columnPB.getFrac(), RoundingMode.HALF_UP);
                        }
                        column.appendDecimal(bd);
                        break;
                    case "CHAR":
                    case "VARCHAR":
                        column.appendString(columnPB.getName() + ":name" + rowId);
                        break;
                }
            }
        }
    }

    @Test
    public void testUnsupportedColumnType(@TempDir Path tempDir) {
        TabletSchema.TabletSchemaPB.Builder schemaBuilder = TabletSchema.TabletSchemaPB.newBuilder()
                .setId(1)
                .setKeysType(TabletSchema.KeysType.DUP_KEYS)
                .setCompressionType(CompressionTypePB.LZ4_FRAME);

        ColumnType[] types = new ColumnType[] {
                new ColumnType("INT", 4),
                new ColumnType("BOOLEAN", 4),
                new ColumnType("TINYINT", 1),
                new ColumnType("SMALLINT", 2),
                new ColumnType("BIGINT", 8),
                new ColumnType("LARGEINT", 16),
                new ColumnType("FLOAT", 4),
                new ColumnType("DOUBLE", 8),
                new ColumnType("DATE", 4),
                new ColumnType("DATETIME", 8),
                new ColumnType("DECIMAL32", 8),
                new ColumnType("DECIMAL64", 16),
                new ColumnType("VARCHAR", 32 + Integer.SIZE),
                new ColumnType("MAP", 16),
                new ColumnType("ARRAY", 16),
                new ColumnType("STRUCT", 16),
                new ColumnType("JSON", 16)};

        int colId = 0;
        for (ColumnType type : types) {
            TabletSchema.ColumnPB.Builder columnBuilder = TabletSchema.ColumnPB.newBuilder();
            columnBuilder
                    .setName("c_" + type.typeName)
                    .setUniqueId(colId)
                    .setIsKey(true)
                    .setIsNullable(false)
                    .setType(type.typeName)
                    .setLength(type.length)
                    .setIndexLength(type.length)
                    .setAggregation("none");
            if (type.typeName.startsWith("DECIMAL32")) {
                columnBuilder.setPrecision(9);
                columnBuilder.setFrac(3);
            } else if (type.typeName.startsWith("DECIMAL64")) {
                columnBuilder.setPrecision(18);
                columnBuilder.setFrac(4);
            } else if (type.typeName.startsWith("DECIMAL128")) {
                columnBuilder.setPrecision(29);
                columnBuilder.setFrac(5);
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

        long tabletId = 100;
        long txnId = 4;
        Map<String, String> options = new HashMap<>();

        String tabletRootPath = tempDir.toAbsolutePath().toString();
        File dir = new File(tabletRootPath + "/data");
        dir.mkdirs();
        dir = new File(tabletRootPath + "/log");
        dir.mkdirs();

        try {
            StarrocksWriter writer = new StarrocksWriter(tabletId,
                    schema,
                    txnId,
                    tabletRootPath,
                    options);
            writer.open();
            Assert.fail("Should not here");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Do not support columns type:[MAP, ARRAY, STRUCT, JSON]"));

        }
    }

    class ColumnType {
        public String typeName;
        public int length;
        public int precision;
        public int scale;

        public ColumnType(String typeName, int length) {
            this.typeName = typeName;
            this.length = length;
        }

        public ColumnType(String typeName, int length, int precision, int scale) {
            this.typeName = typeName;
            this.length = length;
            this.precision = precision;
            this.scale = scale;
        }
    }
}
