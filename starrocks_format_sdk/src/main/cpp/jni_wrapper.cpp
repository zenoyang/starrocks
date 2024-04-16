// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cctz/time_zone.h>
#include <jni.h>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include <glog/logging.h>
#include "column/datum_convert.h"
#include "common/status.h"
#include "format/starrocks_format_chunk.h"
#include "format/starrocks_format_reader.h"
#include "format/starrocks_format_writer.h"
#include "jni_utils.h"
#include "storage/tablet_schema.h"

#include "starrocks_format/starrocks_lib.h"

namespace starrocks::lake::format {

#ifdef __cplusplus
extern "C" {
#endif

static jint jniVersion = JNI_VERSION_1_8;

jclass kJniExceptionClass;
jclass kRuntimeExceptionClass;

jmethodID kJniExceptionConstructor;

jint JNI_OnLoad(JavaVM* vm, void* reserved) {
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion) != JNI_OK) {
        return JNI_ERR;
    }

    kRuntimeExceptionClass = find_class(env, "Ljava/lang/RuntimeException;");
    kJniExceptionClass = find_class(env, "Lcom/starrocks/format/JniException;");

    kJniExceptionConstructor = get_method_id(env, kJniExceptionClass, "<init>", "(ILjava/lang/String;)V");

#ifdef DEBUG
    FLAGS_logtostderr = 1;
#endif
    // logging
    google::InitGoogleLogging("starrocks_format");

    LOG(INFO) << " Jni load successful!";
    starrocks_format_initialize();
    return jniVersion;
}

void JNI_OnUnload(JavaVM* vm, void* reserved) {
    JNIEnv* env;

    vm->GetEnv(reinterpret_cast<void**>(&env), jniVersion);
    env->DeleteGlobalRef(kRuntimeExceptionClass);
    env->DeleteGlobalRef(kJniExceptionClass);

    LOG(INFO) << " Jni unload successful!";
}

JNIEXPORT void JNICALL Java_com_starrocks_format_JniWrapper_releaseWriter(JNIEnv* env, jobject jobj,
                                                                          jlong writerAddress) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(writerAddress);
    LOG(INFO) << "release writer: " << tablet_writer;
    if (tablet_writer != nullptr) {
        delete tablet_writer;
    }
}

JNIEXPORT void JNICALL Java_com_starrocks_format_JniWrapper_releaseReader(JNIEnv* env, jobject jobj,
                                                                          jlong chunkAddress) {
    StarrocksFormatReader* reader = reinterpret_cast<StarrocksFormatReader*>(chunkAddress);
    LOG(INFO) << "release reader: " << reader;
    if (reader != nullptr) {
        delete reader;
    }
}

JNIEXPORT void JNICALL Java_com_starrocks_format_JniWrapper_releaseChunk(JNIEnv* env, jobject jobj,
                                                                         jlong chunkAddress) {
    StarrocksFormatChunk* chunk = reinterpret_cast<StarrocksFormatChunk*>(chunkAddress);
    LOG(INFO) << "release chunk: " << chunk;
    if (chunk != nullptr) {
        delete chunk;
    }
}

// writer functions
JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_createNativeWriter(JNIEnv* env, jobject jobj,
                                                                                     jlong jtablet_id,
                                                                                     jbyteArray jschema, jlong jtxn_id,
                                                                                     jstring jtable_root_path,
                                                                                     jobject joptions) {
    int64_t tablet_id = jtablet_id;
    // get schema
    uint32_t jschema_num_bytes = env->GetArrayLength(jschema);
    int8_t* p_schema = env->GetByteArrayElements(jschema, NULL);
    TabletSchemaPB schema_pb;
    bool parsed = schema_pb.ParseFromArray(p_schema, jschema_num_bytes);
    if (!parsed) {
        LOG(INFO) << " parse schema failed!";
    }
    env->ReleaseByteArrayElements(jschema, p_schema, 0);
    std::shared_ptr<TabletSchema> tablet_schema = TabletSchema::create(schema_pb);

    long txn_id = jtxn_id;
    std::string table_root_path = jstring_to_cstring(env, jtable_root_path);
    std::unordered_map<std::string, std::string> options = jhashmap_to_cmap(env, joptions);

    StarrocksFormatWriter* format_writer =
            new StarrocksFormatWriter(tablet_id, tablet_schema, txn_id, table_root_path, options);

    return reinterpret_cast<int64_t>(format_writer);
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_destroyNativeWriter(JNIEnv* env, jobject jobj,
                                                                                      jlong handler) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, { delete tablet_writer; });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeOpen(JNIEnv* env, jobject jobj, jlong handler) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, { tablet_writer->open(); });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeClose(JNIEnv* env, jobject jobj,
                                                                              jlong handler) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, { tablet_writer->close(); });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeWrite(JNIEnv* env, jobject jobj, jlong handler,
                                                                              jlong jchunk_data) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
        StarrocksFormatChunk* chunk = reinterpret_cast<StarrocksFormatChunk*>(jchunk_data);
        if (chunk == nullptr) {
            LOG(INFO) << "chunk is null";
        }
        if (tablet_writer != nullptr && chunk != nullptr) {
            Status st = tablet_writer->write(chunk);
            // LOG(INFO) << "write result " << st;
            if (!st.ok()) {
                env->ThrowNew(kRuntimeExceptionClass, st.message().get_data());
            }
        }
    });

    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFlush(JNIEnv* env, jobject jobj,
                                                                              jlong handler) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
        Status st = tablet_writer->flush();
        LOG(INFO) << "flush result " << st;
        if (!st.ok()) {
            env->ThrowNew(kRuntimeExceptionClass, st.message().get_data());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_nativeFinish(JNIEnv* env, jobject jobj,
                                                                               jlong handler) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
        Status st = tablet_writer->finish();
        LOG(INFO) << "finish result " << st;
        if (!st.ok()) {
            env->ThrowNew(kRuntimeExceptionClass, st.message().get_data());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksWriter_createNativeChunk(JNIEnv* env, jobject jobj,
                                                                                    jlong handler, jint capacity) {
    StarrocksFormatWriter* tablet_writer = reinterpret_cast<StarrocksFormatWriter*>(handler);
    SAFE_CALL_WRITER_FUNCATION(tablet_writer, {
        StarrocksFormatChunk* chunk = tablet_writer->new_chunk(capacity);
        return reinterpret_cast<int64_t>(chunk);
    });
    return 0;
}

// reader function
JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_createNativeReader(JNIEnv* env, jobject jobj,
                                                                                     jlong jtablet_id, jlong jversion,
                                                                                     jbyteArray jrequired_schema,
                                                                                     jstring jtable_root_path,
                                                                                     jobject joptions) {
    int64_t tablet_id = jtablet_id;
    int64_t version = jversion;
    // get schema
    uint32_t jschema_num_bytes = env->GetArrayLength(jrequired_schema);
    int8_t* p_schema = env->GetByteArrayElements(jrequired_schema, NULL);
    TabletSchemaPB required_schema_pb;
    bool parsed = required_schema_pb.ParseFromArray(p_schema, jschema_num_bytes);
    if (!parsed) {
        LOG(INFO) << " parse schema failed!";
    }
    env->ReleaseByteArrayElements(jrequired_schema, p_schema, 0);
    std::shared_ptr<TabletSchema> required_schema = TabletSchema::create(required_schema_pb);

    std::string table_root_path = jstring_to_cstring(env, jtable_root_path);
    std::unordered_map<std::string, std::string> options = jhashmap_to_cmap(env, joptions);

    StarrocksFormatReader* format_Reader =
            new StarrocksFormatReader(tablet_id, version, required_schema, std::move(table_root_path), options);

    return reinterpret_cast<int64_t>(format_Reader);
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_destroyNativeReader(JNIEnv* env, jobject jobj,
                                                                                      jlong handler) {
    StarrocksFormatReader* tablet_reader = reinterpret_cast<StarrocksFormatReader*>(handler);
    SAFE_CALL_READER_FUNCATION(tablet_reader, { delete tablet_reader; });

    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_nativeOpen(JNIEnv* env, jobject jobj, jlong handler) {
    StarrocksFormatReader* tablet_reader = reinterpret_cast<StarrocksFormatReader*>(handler);
    SAFE_CALL_READER_FUNCATION(tablet_reader, {
        Status st = tablet_reader->open();
        LOG(INFO) << "tablet reader open result " << st;
        if (!st.ok()) {
            env->ThrowNew(kRuntimeExceptionClass, st.message().get_data());
        }
    });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_nativeClose(JNIEnv* env, jobject jobj,
                                                                              jlong handler) {
    StarrocksFormatReader* tablet_reader = reinterpret_cast<StarrocksFormatReader*>(handler);
    SAFE_CALL_READER_FUNCATION(tablet_reader, { tablet_reader->close(); });
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_StarRocksReader_nativeGetNext(JNIEnv* env, jobject jobj,
                                                                                jlong handler) {
    StarrocksFormatReader* tablet_reader = reinterpret_cast<StarrocksFormatReader*>(handler);
    SAFE_CALL_READER_FUNCATION(tablet_reader, {
        StarrocksFormatChunk* format_chunk = tablet_reader->get_next();
        return reinterpret_cast<int64_t>(format_chunk);
    });
    return 0;
}

#ifdef __cplusplus
}
#endif

} // namespace starrocks::lake::format