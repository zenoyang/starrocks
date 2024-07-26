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

#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/tablet_schema.h"

#include "starrocks_format_chunk.h"
#include "storage/lake/tablet.h"

namespace starrocks::lake::format {

class StarRocksFormatWriter {
public:
    StarRocksFormatWriter() = default;
    StarRocksFormatWriter(int64_t tablet_id, std::shared_ptr<TabletSchema>& tablet_schema, int64_t txn_id,
                          std::string tablet_root_path, std::unordered_map<std::string, std::string>& options);

    StarRocksFormatWriter(StarRocksFormatWriter&&) = default;
    StarRocksFormatWriter& operator=(StarRocksFormatWriter&&) = default;

    int64_t tablet_id() { return _tablet_id; };

    std::shared_ptr<TabletSchema>& schema() { return _tablet_schema; };
    const std::shared_ptr<TabletSchema>& schema() const { return _tablet_schema; };

    int64_t txn_id() { return _txn_id; };
    const std::string tablet_root_path() { return _tablet_root_path; };

    Status open();
    void close();
    Status write(StarRocksFormatChunk* chunk);
    Status flush();
    Status finish();

    StarRocksFormatChunk* new_chunk(size_t capacity);

    // write txn log
    Status finish_txn_log();

    // write segment pb
    Status finish_schema_pb();

private:
    Status put_txn_log(const TxnLogPtr& log);
    StatusOr<TabletMetadataPtr> get_tablet_metadata(std::shared_ptr<FileSystem> fs);

private:
    int64_t _tablet_id;
    std::shared_ptr<TabletSchema> _tablet_schema;
    int64_t _txn_id;
    std::string _tablet_root_path;
    std::unordered_map<std::string, std::string> _options;

    std::unique_ptr<Tablet> _tablet;
    WriterType _writer_type;
    uint32_t _max_rows_per_segment;
    std::shared_ptr<FixedLocationProvider> _provider;
    std::unique_ptr<TabletWriter> _tablet_writer;

    bool _share_data = true;
};

} // namespace starrocks::lake
