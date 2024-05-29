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
#include <glog/logging.h>
#include <jni.h>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "format/starrocks_format_chunk.h"
#include "jni_utils.h"

namespace starrocks::lake::format {

#ifdef __cplusplus
extern "C" {
#endif

extern jclass kJniExceptionClass;
extern jclass kRuntimeExceptionClass;

// chunk relate interface
// column relate interface
JNIEXPORT jlong JNICALL Java_com_starrocks_format_Chunk_nativeGetColumn(JNIEnv* env, jobject jobj, jlong handler,
                                                                        jlong index) {
    StarRocksFormatChunk* chunk = reinterpret_cast<StarRocksFormatChunk*>(handler);
    SAFE_CALL_CHUNK_FUNCATION(chunk, {
        StarRocksFormatColumn* column = chunk->get_column_by_index(index);
        return reinterpret_cast<int64_t>(column);
    })
    return 0;
}

JNIEXPORT void JNICALL Java_com_starrocks_format_Chunk_nativeReset(JNIEnv* env, jobject jobj, jlong handler) {
    StarRocksFormatChunk* chunk = reinterpret_cast<StarRocksFormatChunk*>(handler);
    SAFE_CALL_CHUNK_FUNCATION(chunk, { chunk->reset(); })
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_Chunk_nativeColumnCount(JNIEnv* env, jobject jobj, jlong handler) {
    StarRocksFormatChunk* chunk = reinterpret_cast<StarRocksFormatChunk*>(handler);
    SAFE_CALL_CHUNK_FUNCATION(chunk, { return chunk->chunk()->num_columns(); })
    return 0;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_Chunk_nativeNumRows(JNIEnv* env, jobject jobj, jlong handler) {
    StarRocksFormatChunk* chunk = reinterpret_cast<StarRocksFormatChunk*>(handler);
    SAFE_CALL_CHUNK_FUNCATION(chunk, { return chunk->chunk()->num_rows(); })
    return 0;
}

// chunk relate interface
// column relate interface
JNIEXPORT jboolean JNICALL Java_com_starrocks_format_Column_nativeIsNullAt(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jlong index) {
    jboolean jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->column()->is_null(index); });
    return jvalue;
}

JNIEXPORT jboolean JNICALL Java_com_starrocks_format_Column_nativeGetBool(JNIEnv* env, jobject jobj, jlong handler,
                                                                          jlong index) {
    jboolean jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_bool(index); });
    return jvalue;
}

JNIEXPORT jbyte JNICALL Java_com_starrocks_format_Column_nativeGetByte(JNIEnv* env, jobject jobj, jlong handler,
                                                                       jlong index) {
    jbyte jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_byte(index); });
    return jvalue;
}

JNIEXPORT jshort JNICALL Java_com_starrocks_format_Column_nativeGetShort(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jlong index) {
    jshort jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_short(index); });
    return jvalue;
}

JNIEXPORT jint JNICALL Java_com_starrocks_format_Column_nativeGetInt(JNIEnv* env, jobject jobj, jlong handler,
                                                                     jlong index) {
    jint jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_int(index); });
    return jvalue;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_Column_nativeGetLong(JNIEnv* env, jobject jobj, jlong handler,
                                                                       jlong index) {
    jlong jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_long(index); });
    return jvalue;
}

JNIEXPORT jbyteArray JNICALL Java_com_starrocks_format_Column_nativeGetLargeInt(JNIEnv* env, jobject jobj,
                                                                                jlong handler, jlong index) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::vector<uint8_t> value = column->get_largeint(index);

        jbyteArray byteArray = env->NewByteArray(value.size());
        jbyte* bytes = env->GetByteArrayElements(byteArray, NULL);
        for (int i = 0; i < value.size(); i++) {
            bytes[i] = value[i];
        }
        env->ReleaseByteArrayElements(byteArray, bytes, 0);
        return byteArray;
    });
    return 0;
}

JNIEXPORT jfloat JNICALL Java_com_starrocks_format_Column_nativeGetFloat(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jlong index) {
    jfloat jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_float(index); });
    return jvalue;
}

JNIEXPORT jdouble JNICALL Java_com_starrocks_format_Column_nativeGetDouble(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jlong index) {
    jdouble jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_double(index); });
    return jvalue;
}

JNIEXPORT jbyteArray JNICALL Java_com_starrocks_format_Column_nativeGetDecimal(JNIEnv* env, jobject jobj, jlong handler,
                                                                               jlong index) {
    jbyteArray jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::vector<uint8_t> value = column->get_decimal(index);
        jbyteArray byteArray = env->NewByteArray(value.size());
        jbyte* bytes = env->GetByteArrayElements(byteArray, NULL);
        for (int i = 0; i < value.size(); i++) {
            bytes[i] = value[i];
        }
        env->ReleaseByteArrayElements(byteArray, bytes, 0);
        jvalue = byteArray;
    });
    return jvalue;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_Column_nativeGetDate(JNIEnv* env, jobject jobj, jlong handler,
                                                                       jlong index) {
    jlong jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_date(index); });
    return jvalue;
}

JNIEXPORT jlong JNICALL Java_com_starrocks_format_Column_nativeGetTimestamp(JNIEnv* env, jobject jobj, jlong handler,
                                                                            jlong index) {
    jlong jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, { jvalue = column->get_timestamp(index); });
    return jvalue;
}

JNIEXPORT jstring JNICALL Java_com_starrocks_format_Column_nativeGetString(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jlong index) {
    jstring jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::string value = column->get_string(index);
        jvalue = cstring_to_jstring(env, value);
    });
    return jvalue;
}

JNIEXPORT jbyteArray JNICALL Java_com_starrocks_format_Column_nativeGetBinary(JNIEnv* env, jobject jobj, jlong handler,
                                                                              jlong index) {
    jbyteArray jvalue = 0;
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        Slice value = column->get_slice(index);
        jbyteArray byteArray = env->NewByteArray(value.get_size());
        jbyte* bytes = env->GetByteArrayElements(byteArray, NULL);
        for (int i = 0; i < value.get_size(); i++) {
            bytes[i] = value[i];
        }
        env->ReleaseByteArrayElements(byteArray, bytes, 0);
        jvalue = byteArray;
    });
    return jvalue;
}

JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendNull(JNIEnv* env, jobject jobj, jlong handler) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        bool result = column->column()->append_nulls(1);
        if (!result) {
            env->ThrowNew(kRuntimeExceptionClass, "append null failed!");
        }
    });
}

JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendBool(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jboolean jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        bool value = jvalue;
        VLOG(10) << " append bool :" << value;
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendByte(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jbyte jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int8_t value = jvalue;
        VLOG(10) << " append byte :" << value;
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendShort(JNIEnv* env, jobject jobj, jlong handler,
                                                                          jshort jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int16_t value = jvalue;
        VLOG(10) << " append short :" << value;
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendInt(JNIEnv* env, jobject jobj, jlong handler,
                                                                        jint jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int32_t value = jvalue;
        VLOG(10) << " append int :" << value;
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendLong(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jlong jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int64_t value = jvalue;
        VLOG(10) << " append long :" << value;
        column->append_long(value);
    });
}

// The jvalue is in big-endian byte-order. because BigInteger.toByteArray alway return big-endian byte-order byte array
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendLargeInt(JNIEnv* env, jobject jobj, jlong handler,
                                                                             jbyteArray jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int128_t value = BigInteger_to_native_value<int128_t>(env, jvalue);
        VLOG(10) << " append LARGEINT with low bit is :" << (int64_t)(value & 0xFFFFFFFFFFFFFFFF);
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendFloat(JNIEnv* env, jobject jobj, jlong handler,
                                                                          jfloat jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        float value = jvalue;
        VLOG(10) << " append float :" << value;
        column->column()->append_datum(Datum(value));
    });
}
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendDouble(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jdouble jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        double value = jvalue;
        VLOG(10) << " append double :" << value;
        column->column()->append_datum(Datum(value));
    });
}

// the precision and scale of jvalue should same as column's precision and scale.
// jvalue is the int value of decimal.
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendDecimal(JNIEnv* env, jobject jobj, jlong handler,
                                                                            jbyteArray jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::vector<uint8_t> value = jbyteArray_to_carray(env, jvalue);
        column->append_decimal(value);
    });
}

// jvalue is the number of milliseconds since January 1, 1970, 00:00:00 GMT
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendDate(JNIEnv* env, jobject jobj, jlong handler,
                                                                         jlong jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        TimestampValue ts;
        int64_t seconds = jvalue / 1000;
        ts.from_unixtime(seconds, cctz::local_time_zone());

        DateValue value = (DateValue)ts;
        VLOG(10) << " append date :" << value;
        column->column()->append_datum(Datum(value));
    });
}
// jvalue is the number of milliseconds since January 1, 1970, 00:00:00
JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendTimestamp(JNIEnv* env, jobject jobj, jlong handler,
                                                                              jlong jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        int64_t seconds = jvalue / 1000;
        // int64_t microsecond = jvalue % 1000;
        TimestampValue value;
        value.from_unixtime(seconds, TimezoneUtils::local_time_zone());
        VLOG(10) << " append timestamp :" << value;
        column->column()->append_datum(Datum(value));
    });
}

JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendString(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jstring jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::string value = jstring_to_cstring(env, jvalue);
        VLOG(10) << " append string :" << value;
        Status st = column->append_string(value);
        if (!st.ok()) {
            env->ThrowNew(kRuntimeExceptionClass, st.message().get_data());
        }
    });
}

JNIEXPORT void JNICALL Java_com_starrocks_format_Column_nativeAppendBinary(JNIEnv* env, jobject jobj, jlong handler,
                                                                           jbyteArray jvalue) {
    StarRocksFormatColumn* column = reinterpret_cast<StarRocksFormatColumn*>(handler);
    SAFE_CALL_COLUMN_FUNCATION(column, {
        std::vector<uint8_t> value = jbyteArray_to_carray(env, jvalue);
        column->append_binary(value);
    });
}

#ifdef __cplusplus
}
#endif

} // namespace starrocks::lake::format