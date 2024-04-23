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

import com.starrocks.proto.TabletSchema;

import java.util.ArrayList;
import java.util.List;

public class Chunk {

    private long nativeHandler = 0;
    private TabletSchema.TabletSchemaPB outputSchema;
    private List<Column> columns = new ArrayList<>();

    private boolean released = false;

    Chunk(long chunkHandler, TabletSchema.TabletSchemaPB outputSchema) {
        this.nativeHandler = chunkHandler;
        this.outputSchema = outputSchema;
        long chunkColumnCount = columnCount();
        if (columnCount() != outputSchema.getColumnCount()) {
            throw new IllegalArgumentException("Chunk's column count(" + chunkColumnCount
                    + ") is not same as output schema column count (" + outputSchema.getColumnCount() + ")");
        }
        for (int index = 0; index < outputSchema.getColumnCount(); index++) {
            long columnHandler = nativeGetColumn(nativeHandler, index);
            TabletSchema.ColumnPB columnPB = this.outputSchema.getColumn(index);
            Column column = new Column(columnHandler, columnPB);
            columns.add(column);
        }
    }

    public long getNativeHandler() {
        return nativeHandler;
    }

    public TabletSchema.TabletSchemaPB getSchema() {
        return outputSchema;
    }

    public long numRow() {
        return nativeNumRows(nativeHandler);
    }

    public long columnCount() {
        return nativeColumnCount(nativeHandler);
    }

    public Column getColumn(int index) {
        return columns.get(index);
    }

    public void reset() {
        nativeReset(nativeHandler);
    }

    public void release() {
        JniWrapper.get().releaseChunk(nativeHandler);
        released = true;
    }

    private native void nativeReset(long nativeHandler);

    private native long nativeGetColumn(long nativeHandler, long index);

    private native long nativeNumRows(long nativeHandler);

    private native long nativeColumnCount(long nativeHandler);


}
