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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

public class JniLoader {
    private static final Logger LOG = LoggerFactory.getLogger(JniLoader.class);
    public static final String KEY_STARROCKS_FORMAT_JNI_LIB_PATH = "com.starrocks.format.jni.lib.path";
    public static final String KEY_STARROCKS_FORMAT_JNI_LIB_NAME = "com.starrocks.format.jni.lib.name";
    public static final String KEY_STARROCKS_FORMAT_JNI_WRAPPER_NAME = "com.starrocks.format.jni.wrapper.name";

    private static volatile boolean isLoaded = false;

    public static synchronized void loadNativeLibrary(String version) {
        if (isLoaded) {
            return;
        }
        LOG.info("begin load native StarRocks format library.");
        long start = System.currentTimeMillis();
        try {
            // load libhdfs.so.0.0.0
            // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
            String nativeLibName = System.mapLibraryName("hdfs") + ".0.0.0";
            File nativeLibFile = findNativeLibrary(nativeLibName, version);
            System.load(nativeLibFile.getAbsolutePath());
  
            // load libstarrocks_format.so
            nativeLibName = System.getProperty(KEY_STARROCKS_FORMAT_JNI_LIB_NAME);
            // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
            if (nativeLibName == null) {
                nativeLibName = System.mapLibraryName("starrocks_format");
            }
            nativeLibFile = findNativeLibrary(nativeLibName, version);
            System.load(nativeLibFile.getAbsolutePath());

            // load starrocks_format_wrapper.so
            nativeLibName = System.getProperty(KEY_STARROCKS_FORMAT_JNI_WRAPPER_NAME);
            // Resolve the library file name with a suffix (e.g., dll, .so, etc.)
            if (nativeLibName == null) {
                nativeLibName = System.mapLibraryName("starrocks_format_wrapper");
            }
            nativeLibFile = findNativeLibrary(nativeLibName, version);
            System.load(nativeLibFile.getAbsolutePath());
        } catch (Exception e) {
            throw e;
        }
        long time = System.currentTimeMillis() - start;
        LOG.info("finished load native StarRocks format library. taken " + time + " ms");
        isLoaded = true;
    }

    private static File findNativeLibrary(String nativeLibName, String version) {

        // Try to load the library in provided existed path  */
        String jniNativeLibraryPath = System.getProperty(KEY_STARROCKS_FORMAT_JNI_LIB_PATH);
        if (jniNativeLibraryPath != null) {
            File nativeLib = new File(jniNativeLibraryPath, nativeLibName);
            if (nativeLib.exists()) {
                LOG.info("Found native StarRocks format library: " + nativeLib.toPath());
                return nativeLib;
            }
        }
        // Load a native library inside a jar file
        // Temporary folder for the native lib. Use the value of org.xerial.snappy.tempdir or java.io.tmpdir
        File tempFolder = new File(System.getProperty("java.io.tmpdir"));
        if (!tempFolder.exists()) {
            boolean created = tempFolder.mkdirs();
            if (!created) {
                // if created == false, it will fail eventually in the later part
            }
        }

        // Extract and load a native library inside the jar file
        return extractLibraryFile(nativeLibName, version, tempFolder.getAbsolutePath());
    }


    private static File extractLibraryFile(String libraryFileName, String libraryVersion, String targetFolder) {
        String nativeLibraryFilePath = "native/" + libraryFileName;
        String extractedLibFileName = String.format("%s-%s", libraryFileName, libraryVersion);
        File extractedLibFile = new File(targetFolder, extractedLibFileName);
        if (extractedLibFile.exists()) {
            LOG.info("Found native StarRocks format library: " + extractedLibFile.toPath());
            return extractedLibFile;
        }

        File extractedLibFileLock = null;
        boolean success = false;
        try {
            // Create the .lck file first to avoid a race condition
            // with other concurrently running Java processes.
            extractedLibFileLock = File.createTempFile(extractedLibFileName + "-", ".lck");

            // Extract a native library file into the target directory
            try (final InputStream reader = JniLoader.class.getClassLoader().getResourceAsStream(nativeLibraryFilePath)) {
                if (reader == null) {
                    throw new FileNotFoundException(nativeLibraryFilePath);
                }
                Files.copy(reader, extractedLibFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }

            // Set executable (x) flag to enable Java to load the native library
            success = extractedLibFile.setReadable(true) &&
                    extractedLibFile.setWritable(true, true) &&
                    extractedLibFile.setExecutable(true);

            // Check whether the contents are properly copied from the resource folder
            try (InputStream nativeIn = JniLoader.class.getClassLoader().getResourceAsStream(nativeLibraryFilePath);
                    InputStream extractedLibIn = Files.newInputStream(extractedLibFile.toPath())) {
                if (!contentsEquals(nativeIn, extractedLibIn)) {
                    throw new RuntimeException(String.format("Failed to write a native library file at %s", extractedLibFile));
                }
            }
            LOG.info("Successfully decomppress native StarRocks format library: " + extractedLibFile.toPath());
            return new File(targetFolder, extractedLibFileName);
        } catch (IOException e) {
            throw new RuntimeException("extract " + extractedLibFileName + " failed!", e);
        } finally {
            if (success) {
                extractedLibFileLock.deleteOnExit();
            } else {
                if (extractedLibFile.exists()) {
                    if (!extractedLibFile.delete()) {
                        throw new ExceptionInInitializerError("Cannot unpack starrocks format native library /" +
                                " cannot delete a temporary native library " + extractedLibFile);
                    }
                }
                if (extractedLibFileLock != null && extractedLibFileLock.exists()) {
                    if (!extractedLibFileLock.delete()) {
                        throw new ExceptionInInitializerError("Cannot unpack starrocks format native library /" +
                                " cannot delete a temporary lock file " + extractedLibFileLock);
                    }
                }
            }
        }
    }

    private static boolean contentsEquals(InputStream in1, InputStream in2)
            throws IOException {
        if (!(in1 instanceof BufferedInputStream)) {
            in1 = new BufferedInputStream(in1);
        }
        if (!(in2 instanceof BufferedInputStream)) {
            in2 = new BufferedInputStream(in2);
        }

        int ch = in1.read();
        while (ch != -1) {
            int ch2 = in2.read();
            if (ch != ch2) {
                return false;
            }
            ch = in1.read();
        }
        int ch2 = in2.read();
        return ch2 == -1;
    }

}
