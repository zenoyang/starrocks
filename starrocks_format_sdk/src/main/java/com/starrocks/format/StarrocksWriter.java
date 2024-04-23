package com.starrocks.format;

import com.starrocks.proto.TabletSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarrocksWriter {
    static JniWrapper jniWrapper = JniWrapper.get();
    private long tabletId;
    private TabletSchema.TabletSchemaPB schema;
    private long txnId;
    private String tabletRootPath;
    private Map<String, String> options;
    // nativeWriter is the c++ StarrocksFormatWriter potiner
    private long nativeWriter = 0;

    private boolean released = false;

    public StarrocksWriter(long tabletId, TabletSchema.TabletSchemaPB schema, long txnId,
                           String tabletRootPath, Map<String, String> options) {
        checkSchema(schema);
        this.tabletId = tabletId;
        this.schema = schema;
        this.txnId = txnId;
        this.tabletRootPath = tabletRootPath;
        this.options = options;
        nativeWriter = createNativeWriter(tabletId,
                schema.toByteArray(),
                txnId,
                tabletRootPath,
                options);
    }

    private void checkSchema(TabletSchema.TabletSchemaPB schema) {
        if (schema == null || schema.getColumnCount() == 0) {
            throw new RuntimeException("Schema should not be empty!");
        }
        List<TabletSchema.ColumnPB> notSupportColumns = schema.getColumnList().stream()
                .filter(x -> !Column.SUPPORT_COLUMN_TYPE.contains(x.getType()))
                .collect(Collectors.toList());
        if (notSupportColumns.size() > 0) {
            String columnTypes = notSupportColumns.stream().map(x -> x.getType()).collect(Collectors.joining(", "));
            throw new RuntimeException("Do not support columns type:[" + columnTypes + "]");
        }
    }

    public void open() {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        nativeOpen(nativeWriter);
    }

    public void close() {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        nativeClose(nativeWriter);
    }

    public long write(Chunk chunk) {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        return nativeWrite(nativeWriter, chunk.getNativeHandler());
    }

    public long flush() {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        return nativeFlush(nativeWriter);
    }


    public long finish() {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        return nativeFinish(nativeWriter);
    }

    public Chunk newChunk(int capacity) {
        if (nativeWriter == 0 || released) {
            throw new RuntimeException("Illegal native writer!");
        }
        long chunkHandler = createNativeChunk(nativeWriter, capacity);
        return new Chunk(chunkHandler, schema);
    }

    public void release() {
        JniWrapper.get().releaseWriter(nativeWriter);
        nativeWriter = 0;
        released = true;
    }

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
