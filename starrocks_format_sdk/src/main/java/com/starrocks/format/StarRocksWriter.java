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

public class StarRocksWriter {

    static JniWrapper jniWrapper = JniWrapper.get();

    private final Long tabletId;
    private final TabletSchemaPB schema;
    private final Long txnId;
    private final String tabletRootPath;
    private final Map<String, String> options;

    // nativeWriter is the c++ StarRocksFormatWriter potiner
    private long nativeWriter = 0;

    private volatile boolean released = false;

    public StarRocksWriter(long tabletId,
                           TabletSchemaPB schema,
                           long txnId,
                           String tabletRootPath,
                           Map<String, String> options) {
        checkSchema(schema);
        this.tabletId = tabletId;
        this.schema = schema;
        this.txnId = txnId;
        this.tabletRootPath = tabletRootPath;
        this.options = options;
        this.nativeWriter = createNativeWriter(
                tabletId,
                schema.toByteArray(),
                txnId,
                tabletRootPath,
                options);
    }

    public void open() {
        checkState();
        nativeOpen(nativeWriter);
    }

    public void close() {
        checkState();
        nativeClose(nativeWriter);
    }

    public long write(Chunk chunk) {
        checkState();
        return nativeWrite(nativeWriter, chunk.getNativeHandler());
    }

    public long flush() {
        checkState();
        return nativeFlush(nativeWriter);
    }


    public long finish() {
        checkState();
        return nativeFinish(nativeWriter);
    }

    public Chunk newChunk(int capacity) {
        checkState();
        long chunkHandler = createNativeChunk(nativeWriter, capacity);
        return new Chunk(chunkHandler, schema);
    }

    public void release() {
        JniWrapper.get().releaseWriter(nativeWriter);
        nativeWriter = 0;
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
        if (0 == nativeWriter) {
            throw new IllegalStateException("Native writer may not be created correctly.");
        }

        if (released) {
            throw new IllegalStateException("Native writer is released.");
        }
    }

    /* native methods */

    public native long createNativeWriter(long tabletId,
                                          byte[] schemaPb,
                                          long txnId,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeWriter);

    public native long nativeWrite(long nativeWriter, long chunkAddr);

    public native long nativeFlush(long nativeWriter);

    public native long nativeFinish(long nativeWriter);

    public native long nativeClose(long nativeWriter);

    public native long createNativeChunk(long nativeWriter, int capacity);
}
