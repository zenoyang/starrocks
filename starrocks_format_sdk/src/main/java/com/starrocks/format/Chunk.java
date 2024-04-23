package com.starrocks.format;

import com.starrocks.proto.TabletSchema;

import java.util.ArrayList;
import java.util.List;

public class Chunk {

    private long nativeHandler = 0;
    private TabletSchema.TabletSchemaPB schema;
    private List<Column> columns = new ArrayList<Column>();

    private boolean released = false;

    Chunk(long chunkHandler, TabletSchema.TabletSchemaPB schema) {
        this.nativeHandler = chunkHandler;
        this.schema = schema;
        long chunkColumnCount = columnCount();
        if (columnCount() != schema.getColumnCount()) {
            throw new IllegalArgumentException("Chunk's column count(" + chunkColumnCount
                    + ") is not same as schema column count" + schema.getColumnCount() + ")");
        }
        for (int index = 0; index < schema.getColumnCount(); index++) {
            long columnHandler = nativeGetColumn(nativeHandler, index);
            TabletSchema.ColumnPB columnPB = this.schema.getColumn(index);
            Column column = new Column(columnHandler, columnPB);
            columns.add(column);
        }
    }

    public long getNativeHandler() {
        return nativeHandler;
    }

    public TabletSchema.TabletSchemaPB getSchema() {
        return schema;
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
