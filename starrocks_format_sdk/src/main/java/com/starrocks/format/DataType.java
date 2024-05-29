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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum DataType {

    BOOLEAN(1, "BOOLEAN"),
    TINYINT(2, "TINYINT"),
    SMALLINT(3, "SMALLINT"),
    INT(4, "INT"),
    BIGINT(5, "BIGINT"),
    LARGEINT(6, "LARGEINT"),
    FLOAT(7, "FLOAT"),
    DOUBLE(8, "DOUBLE"),
    DECIMAL(9, "DECIMAL"),
    DECIMAL32(10, "DECIMAL32"),
    DECIMAL64(11, "DECIMAL64"),
    DECIMAL128(12, "DECIMAL128"),
    CHAR(13, "CHAR"),
    VARCHAR(14, "VARCHAR"),
    BINARY(15, "BINARY"),
    VARBINARY(16, "VARBINARY"),
    DATE(17, "DATE"),
    DATETIME(18, "DATETIME"),
    ARRAY(19, "ARRAY", false),
    JSON(20, "JSON"),
    MAP(21, "MAP", false),
    STRUCT(22, "STRUCT", false);

    private static final Map<String, DataType> REVERSED_MAPPING = new HashMap<>(values().length);

    static {
        for (DataType dataType : values()) {
            String typeLiteral = dataType.getLiteral().toUpperCase();
            if (REVERSED_MAPPING.containsKey(typeLiteral)) {
                throw new IllegalStateException("duplicated data type literal: " + dataType.getLiteral());
            }

            REVERSED_MAPPING.put(typeLiteral, dataType);
        }
    }

    private final int id;
    private final String literal;
    private final boolean supported;

    DataType(int id, String literal) {
        this(id, literal, true);
    }

    DataType(int id, String literal, boolean supported) {
        this.id = id;
        this.literal = literal;
        this.supported = supported;
    }

    public static Optional<DataType> fromLiteral(String literal) {
        return Optional.ofNullable(literal).map(l -> REVERSED_MAPPING.get(l.toUpperCase()));
    }

    public static boolean isSupported(String dataType) {
        if (null == dataType) {
            return false;
        }

        DataType dt = REVERSED_MAPPING.get(dataType.toUpperCase());
        return null != dt && dt.isSupported();
    }

    public static boolean isUnsupported(String dataType) {
        return !isSupported(dataType);
    }

    public int getId() {
        return id;
    }

    public String getLiteral() {
        return literal;
    }

    public boolean isSupported() {
        return supported;
    }
}
