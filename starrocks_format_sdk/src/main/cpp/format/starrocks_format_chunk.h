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

#pragma once

#define FMT_HEADER_ONLY

#include "column/column.h"
#include "column/type_traits.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake::format {

class StarRocksFormatColumn {
public:
    StarRocksFormatColumn(Column* column, Field* field) : _column(column), _field(field){};

    StarRocksFormatColumn(StarRocksFormatColumn&&) = default;
    StarRocksFormatColumn& operator=(StarRocksFormatColumn&&) = default;

    Column* column() { return _column; };
    Field* field() { return _field; };

    void append_long(int64_t value);
    void append_decimal(std::vector<uint8_t>& value);
    Status append_string(std::string& value);

    int8_t get_bool(size_t index);
    int8_t get_byte(size_t index);
    int16_t get_short(size_t index);
    int32_t get_int(size_t index);
    std::vector<uint8_t> get_largeint(size_t index);
    int64_t get_long(size_t index);
    float get_float(size_t index);
    double get_double(size_t index);
    std::vector<uint8_t> get_decimal(size_t index);
    int64_t get_date(size_t index);
    int64_t get_timestamp(size_t index);

    template <LogicalType type>
    RunTimeCppType<type> get_fixlength_column_value(size_t index);

    std::string get_string(size_t index);

private:
    // we should not maintain _column and _field 's memory,
    // because the memory of _column is maintained by StarRocksFormatChunk's member _chunk
    // the memory of _column will be released in _chunk's destructor,
    // release StarRocksFormatChunk will release it member _chunk and _columns
    Column* _column;
    Field* _field;
};

class StarRocksFormatChunk {
public:
    StarRocksFormatChunk(ChunkUniquePtr chunk);
    StarRocksFormatChunk(std::shared_ptr<starrocks::Schema> schema, size_t capacity);

    StarRocksFormatChunk(StarRocksFormatChunk&&) = default;
    StarRocksFormatChunk& operator=(StarRocksFormatChunk&&) = default;

    void resolve_columns(ChunkUniquePtr& chunk);
    ChunkUniquePtr& chunk() { return _chunk; };

    StarRocksFormatColumn* get_column_by_index(size_t idx);
    void reset();
private:
    ChunkUniquePtr _chunk;
    std::vector<std::unique_ptr<StarRocksFormatColumn>> _columns;
};

} // namespace starrocks::lake
