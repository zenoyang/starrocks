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

import com.starrocks.proto.TabletSchema.ColumnPB;
import com.starrocks.proto.TabletSchema.TabletSchemaPB;

import java.util.Map;

public class StarRocksReader {

    static JniWrapper jniWrapper = JniWrapper.get();

    private final Long tabletId;
    private final TabletSchemaPB schema;
    private final String tabletRootPath;
    private final Map<String, String> options;

    // nativeReader is the c++ StarRocksFormatReader potiner
    private long nativeReader = 0;

    private volatile boolean released = false;

    public StarRocksReader(long tabletId,
                           long version,
                           TabletSchemaPB schema,
                           String tabletRootPath,
                           Map<String, String> options) {
        checkSchema(schema);
        this.tabletId = tabletId;
        this.schema = schema;
        this.tabletRootPath = tabletRootPath;
        this.options = options;
        this.nativeReader = createNativeReader(
                tabletId,
                version,
                schema.toByteArray(),
                tabletRootPath,
                options);
    }

    public void open() {
        checkState();
        nativeOpen(nativeReader);
    }

    public void close() {
        checkState();
        nativeClose(nativeReader);
    }

    public Chunk getNext() {
        checkState();
        long chunkHandler = nativeGetNext(nativeReader);
        return new Chunk(chunkHandler, schema);
    }

    public void release() {
        JniWrapper.get().releaseReader(nativeReader);
        nativeReader = 0;
        released = true;
    }

    private static void checkSchema(TabletSchemaPB schema) {
        if (schema == null || schema.getColumnCount() == 0) {
            throw new IllegalArgumentException("Schema should not be empty.");
        }

        for (ColumnPB column : schema.getColumnList()) {
            if (DataType.isUnsupported(column.getType())) {
                throw new UnsupportedOperationException("Unsupported column type: " + column.getType());
            }
        }
    }

    private void checkState() {
        if (0 == nativeReader) {
            throw new IllegalStateException("Native reader may not be created correctly.");
        }

        if (released) {
            throw new IllegalStateException("Native reader is released.");
        }
    }

    /* native methods */

    public native long createNativeReader(long tabletId,
                                          long version,
                                          byte[] schemaPb,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeReader);

    public native long nativeClose(long nativeReader);

    public native long nativeGetNext(long nativeReader);
}
