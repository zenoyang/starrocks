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