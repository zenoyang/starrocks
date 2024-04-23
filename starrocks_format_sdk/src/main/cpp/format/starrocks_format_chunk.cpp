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

#include <glog/logging.h>

#include "format_utils.h"
#include "starrocks_format_chunk.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "storage/chunk_helper.h"

namespace starrocks::lake::format {

StarRocksFormatChunk::StarRocksFormatChunk(ChunkUniquePtr chunk) : _chunk(std::move(chunk)) {
    resolve_columns(_chunk);
}

StarRocksFormatChunk::StarRocksFormatChunk(std::shared_ptr<TabletSchema> tablet_schema, size_t capacity) {
    _chunk = ChunkHelper::new_chunk(ChunkHelper::convert_schema(tablet_schema), capacity);
    resolve_columns(_chunk);
}

void StarRocksFormatChunk::resolve_columns(ChunkUniquePtr& chunk) {
    _columns.resize(_chunk->num_columns());
    for (auto idx = 0; idx < _chunk->num_columns(); idx++) {
        auto column = std::make_unique<StarRocksFormatColumn>(_chunk->get_column_by_index(idx).get(),
                                                              _chunk->schema()->field(idx).get());
        _columns[idx] = std::move(column);
    }
}

StarRocksFormatColumn* StarRocksFormatChunk::get_column_by_index(size_t idx) {
    DCHECK_GE(idx, 0);
    DCHECK_LT(idx, _columns.size());
    return _columns[idx].get();
}

void StarRocksFormatChunk::reset() {
    _chunk->reset();
}

void StarRocksFormatColumn::append_long(int64_t value) {
    _column->append_datum(value);
}

// value is big endian byte vector
void StarRocksFormatColumn::append_decimal(std::vector<uint8_t>& value) {
    switch (_field->type()->type()) {
    case LogicalType::TYPE_DECIMAL32: {
        auto cpp_value = big_endian_bytes_to_native_value<int32_t>(value);
        _column->append_datum((int32_t)cpp_value);
    } break;
    case LogicalType::TYPE_DECIMAL64: {
        auto cpp_value = big_endian_bytes_to_native_value<int64_t>(value);
        _column->append_datum(cpp_value);
    } break;
    case LogicalType::TYPE_DECIMAL128: {
        auto cpp_value = big_endian_bytes_to_native_value<int128_t>(value);
        _column->append_datum(cpp_value);
    } break;
    default:
        LOG(WARNING) << "should not here.";
        break;
    }
}

int8_t StarRocksFormatColumn::get_bool(size_t index) {
    return get_fixlength_column_value<TYPE_BOOLEAN>(index);
}

int8_t StarRocksFormatColumn::get_byte(size_t index) {
    return get_fixlength_column_value<TYPE_TINYINT>(index);
}

int16_t StarRocksFormatColumn::get_short(size_t index) {
    return get_fixlength_column_value<TYPE_SMALLINT>(index);
}

int32_t StarRocksFormatColumn::get_int(size_t index) {
    return get_fixlength_column_value<TYPE_INT>(index);
}

int64_t StarRocksFormatColumn::get_long(size_t index) {
    return get_fixlength_column_value<TYPE_BIGINT>(index);
}

// return big endian byte vector
std::vector<uint8_t> StarRocksFormatColumn::get_largeint(size_t index) {
    int128_t value = get_fixlength_column_value<TYPE_LARGEINT>(index);
    return to_big_endian_bytes<int128_t>(value);
}

float StarRocksFormatColumn::get_float(size_t index) {
    return get_fixlength_column_value<TYPE_FLOAT>(index);
}

double StarRocksFormatColumn::get_double(size_t index) {
    return get_fixlength_column_value<TYPE_DOUBLE>(index);
}

// return big endian byte vector
std::vector<uint8_t> StarRocksFormatColumn::get_decimal(size_t index) {
    switch (_field->type()->type()) {
    case LogicalType::TYPE_DECIMAL32: {
        int32_t value = get_fixlength_column_value<TYPE_DECIMAL32>(index);
        return to_big_endian_bytes<int32_t>(value);
    }
    case LogicalType::TYPE_DECIMAL64: {
        int64_t value = get_fixlength_column_value<TYPE_DECIMAL64>(index);
        return to_big_endian_bytes<int64_t>(value);
    }
    case LogicalType::TYPE_DECIMAL128: {
        int128_t value = get_fixlength_column_value<TYPE_DECIMAL128>(index);
        return to_big_endian_bytes<int128_t>(value);
    } break;
    default:
        std::string error_msg = "Unsupported type:" + type_to_string_v2(_field->type()->type());
        LOG(WARNING) << error_msg;
        throw std::runtime_error(error_msg);
    }
}

int64_t StarRocksFormatColumn::get_date(size_t index) {
    DateValue value = get_fixlength_column_value<TYPE_DATE>(index);
    return value.to_unixtime();
}

int64_t StarRocksFormatColumn::get_timestamp(size_t index) {
    TimestampValue value = get_fixlength_column_value<TYPE_DATETIME>(index);
    return value.to_unixtime();
}

template <LogicalType type>
RunTimeCppType<type> StarRocksFormatColumn::get_fixlength_column_value(size_t index) {
    using CppType = RunTimeCppType<type>;
    using ColumnType = RunTimeColumnType<type>;
    auto* data_column = ColumnHelper::get_data_column(_column);
    const CppType* column_data = down_cast<const ColumnType*>(data_column)->get_data().data();
    return column_data[index];
}

std::string StarRocksFormatColumn::get_string(size_t index) {
    auto* data_column = ColumnHelper::get_data_column(_column);
    Slice slice = down_cast<const BinaryColumn*>(data_column)->get_slice(index);
    return slice.to_string();
}

} // namespace starrocks::lake::format
