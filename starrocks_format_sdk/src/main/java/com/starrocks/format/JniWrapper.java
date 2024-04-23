package com.starrocks.format;

public class JniWrapper {
    public static final String JNI_LIB_VERSION = "3.2.3-ve-0";

    private static final JniWrapper INSTANCE = new JniWrapper();

    public static JniWrapper get() {
        JniLoader.loadNativeLibrary(JNI_LIB_VERSION);
        return INSTANCE;
    }

    private JniWrapper() {
    }

    public native void releaseWriter(long writerAddress);

    public native void releaseReader(long readerAddress);

    public native void releaseChunk(long chunkAddress);


}
