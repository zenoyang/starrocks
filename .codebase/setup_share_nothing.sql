drop database if EXISTS demo;

create database demo;
use demo;

-- segment export test
CREATE TABLE `tb_all_primitivetype_write_duplicate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

create table tb_all_primitivetype_write_duplicate2 like tb_all_primitivetype_write_duplicate;

CREATE TABLE `tb_all_primitivetype_write_unique` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

create table tb_all_primitivetype_write_unique2 like tb_all_primitivetype_write_unique;

CREATE TABLE `tb_all_primitivetype_write_primary` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

create table tb_all_primitivetype_write_primary2 like tb_all_primitivetype_write_primary;

CREATE TABLE `tb_all_primitivetype_write_aggregate` (
  `rowId` int(11) NULL COMMENT "",
  `c_boolean` BOOLEAN REPLACE NULL COMMENT "",
  `c_tinyint` TINYINT MAX NULL COMMENT "",
  `c_smallint` SMALLINT MAX NULL COMMENT "",
  `c_int` int(11) SUM NULL COMMENT "",
  `c_bigint` BIGINT SUM NULL COMMENT "",
  `c_float` FLOAT SUM NULL COMMENT "",
  `c_double` DOUBLE SUM NULL COMMENT "",
  `c_date` DATE MAX NULL COMMENT "",
  `c_datetime` DATETIME MAX NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) SUM NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  SUM NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  SUM NULL COMMENT "",
  `c_char` CHAR(128)  MAX NULL COMMENT "",
  `c_varchar` VARCHAR(512)  MAX NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);

create table tb_all_primitivetype_write_aggregate2 like tb_all_primitivetype_write_aggregate;

CREATE TABLE `tb_fast_schema_change_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"fast_schema_evolution" = "true",
"replication_num" = "1"
);

CREATE TABLE `tb_no_fast_schema_change_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_compress_lz4` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"compression" = "LZ4",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_compress_zstd` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"compression" = "zstd",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_compress_zlib` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"compression" = "zlib",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_compress_snappy` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"compression" = "snappy",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_order_by_int` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
ORDER BY(`c_bigint`,`c_date`)
PROPERTIES (
"compression" = "snappy",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_order_by_varchar` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
COMMENT "OLAP"
DISTRIBUTED BY HASH(rowId) BUCKETS 1
ORDER BY(`c_varchar`,`c_boolean`)
PROPERTIES (
"compression" = "snappy",
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_rename_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_swap_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

create table tb_all_primitivetype_swap_table1 like tb_all_primitivetype_swap_table;

CREATE TABLE `tb_all_primitivetype_swap_diff_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_swap_diff_table1` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_modify_comment` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_storage_hdd` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY KEY(rowId)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
);

CREATE TABLE `tb_all_primitivetype_partition_dup_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE  KEY(rowId)
PARTITION BY RANGE(c_date) (
PARTITION p20240824 VALUES LESS THAN ("2024-08-24"),
PARTITION p20240825 VALUES LESS THAN ("2024-08-25"),
PARTITION p20240826 VALUES LESS THAN ("2024-08-26"),
PARTITION p20240827 VALUES LESS THAN ("2024-08-27"),
PARTITION p20240828 VALUES LESS THAN ("2024-08-28")
)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_partition_primary_table` (
  `rowId` int(11) COMMENT "",
  `c_date` DATE COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_char` CHAR(128)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY  KEY(rowId, c_date)
PARTITION BY RANGE(c_date) (
PARTITION p20240824 VALUES LESS THAN ("2024-08-24"),
PARTITION p20240825 VALUES LESS THAN ("2024-08-25"),
PARTITION p20240826 VALUES LESS THAN ("2024-08-26"),
PARTITION p20240827 VALUES LESS THAN ("2024-08-27"),
PARTITION p20240828 VALUES LESS THAN ("2024-08-28")
)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_list_partition_dup_table` (
  `rowId` int(11) COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_date` DATE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_string` CHAR(128) NOT NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE  KEY(rowId)
PARTITION BY LIST(c_string) (
PARTITION pUSA VALUES IN ("Los Angeles","San Francisco","San Diego"),
PARTITION pChina VALUES IN ("Beijing","Shanghai","Hangzhou")
)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);

CREATE TABLE `tb_all_primitivetype_list_partition_primary_table` (
  `rowId` int(11) COMMENT "",
  `c_string` string NOT NULL COMMENT "",
  `c_date` DATE COMMENT "",
  `c_boolean` BOOLEAN NULL COMMENT "",
  `c_tinyint` TINYINT NULL COMMENT "",
  `c_smallint` SMALLINT NULL COMMENT "",
  `c_int` int(11) NULL COMMENT "",
  `c_bigint` BIGINT NULL COMMENT "",
  `c_float` FLOAT NULL COMMENT "",
  `c_double` DOUBLE NULL COMMENT "",
  `c_datetime` DATETIME NULL COMMENT "",
  `c_decimal32` DECIMAL32(9, 2) NULL COMMENT "",
  `c_decimal64` DECIMAL(18, 3)  NULL COMMENT "",
  `c_decimal128` DECIMAL(38, 4)  NULL COMMENT "",
  `c_varchar` VARCHAR(512)  NULL COMMENT ""
) ENGINE=OLAP
PRIMARY  KEY(rowId, c_string)
PARTITION BY LIST(c_string) (
PARTITION pUSA VALUES IN ("Los Angeles","San Francisco","San Diego"),
PARTITION pChina VALUES IN ("Beijing","Shanghai","Hangzhou")
)
DISTRIBUTED BY HASH(rowId) BUCKETS 1
PROPERTIES (
"replication_num" = "1"
);