package com.starrocks.format;

import com.starrocks.proto.TabletSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StarrocksReader {
    static JniWrapper jniWrapper = JniWrapper.get();
    private long tabletId;
    private TabletSchema.TabletSchemaPB schema;
    private String tabletRootPath;
    private Map<String, String> options;
    // nativeReader is the c++ StarrocksFormatReader potiner
    private long nativeReader = 0;

    private boolean released = false;

    public StarrocksReader(long tabletId, long version, TabletSchema.TabletSchemaPB schema,
                           String tabletRootPath, Map<String, String> options) {
        checkSchema(schema);
        this.tabletId = tabletId;
        this.schema = schema;
        this.tabletRootPath = tabletRootPath;
        this.options = options;
        nativeReader = createNativeReader(tabletId,
                version,
                schema.toByteArray(),
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
        if (nativeReader == 0 || released) {
            throw new RuntimeException("Illegal native reader!");
        }
        nativeOpen(nativeReader);
    }

    public void close() {
        if (nativeReader == 0 || released) {
            throw new RuntimeException("Illegal native reader!");
        }
        nativeClose(nativeReader);
    }

    public Chunk getNext() {
        if (nativeReader == 0 || released) {
            throw new RuntimeException("Illegal native reader!");
        }
        long chunkHandler = nativeGetNext(nativeReader);
        return new Chunk(chunkHandler, schema);
    }

    public void release() {
        JniWrapper.get().releaseReader(nativeReader);
        nativeReader = 0;
        released = true;
    }

    public native long createNativeReader(long tabletId,
                                          long version,
                                          byte[] schemaPb,
                                          String tableRootPath,
                                          Map<String, String> options);

    public native long nativeOpen(long nativeReader);

    public native long nativeClose(long nativeReader);

    public native long nativeGetNext(long nativeReader);
}
