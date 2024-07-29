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

#include "starrocks_format_writer.h"

#include <glog/logging.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/status.h"
#include "format_utils.h"
#include "starrocks_format/starrocks_lib.h"
#include "storage/chunk_helper.h"
#include "storage/lake/join_path.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_writer.h"
#include "storage/protobuf_file.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake::format {

StarRocksFormatWriter::StarRocksFormatWriter(int64_t tablet_id, std::shared_ptr<TabletSchema>& tablet_schema,
                                             int64_t txn_id, std::string tablet_root_path,
                                             std::unordered_map<std::string, std::string>& options)
        : _tablet_id(tablet_id),
          _tablet_schema(tablet_schema),
          _txn_id(txn_id),
          _tablet_root_path(tablet_root_path),
          _options(options) {
    _provider = std::make_shared<FixedLocationProvider>(tablet_root_path);

    _writer_type = WriterType::kHorizontal;
    auto it = _options.find("starrocks.format.writer_type");
    if (it != _options.end()) {
        std::string writer_type = it->second;
        _writer_type = WriterType(std::stoi(writer_type));
    }
    auto mode_it = _options.find("starrocks.format.mode");
    if (mode_it != _options.end()) {
        std::string mode = mode_it->second;
        if (mode == "share_nothing") {
            _share_data = false;
        }
    }
    _max_rows_per_segment =
            getIntOrDefault(_options, "starrocks.format.rows_per_segment", std::numeric_limits<uint32_t>::max());
}

Status StarRocksFormatWriter::open() {
    if (!_tablet_writer) {
        // support the below file system options, same as hadoop aws fs options
        // fs.s3a.path.style.access
        // fs.s3a.access.key
        // fs.s3a.secret.key
        // fs.s3a.endpoint
        // fs.s3a.endpoint.region
        // fs.s3a.connection.ssl.enabled
        // fs.s3a.retry.limit
        // fs.s3a.retry.interval
        auto fs_options = filter_map_by_key_prefix(_options, "fs.");
        ASSIGN_OR_RETURN(auto fs, FileSystem::Create(_tablet_root_path, FSOptions(fs_options)));
        if (_share_data) {
            // get tablet schema;
            ASSIGN_OR_RETURN(auto metadata, get_tablet_metadata(fs));
            _tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
        }

        _tablet = std::make_unique<Tablet>(_lake_tablet_manager, _tablet_id, _provider, _tablet_schema);
        // create tablet writer
        ASSIGN_OR_RETURN(_tablet_writer, _tablet->new_writer(_writer_type, _txn_id, _max_rows_per_segment));
        _tablet_writer->set_fs(fs);
        _tablet_writer->set_location_provider(_provider);
    }
    return _tablet_writer->open();
}

void StarRocksFormatWriter::close() {
    _tablet_writer->close();
}

Status StarRocksFormatWriter::write(StarRocksFormatChunk* chunk) {
    if (chunk != nullptr && chunk->chunk()->num_rows() > 0) {
        return _tablet_writer->write(*chunk->chunk().get());
    }
    return Status::OK();
}

Status StarRocksFormatWriter::flush() {
    return _tablet_writer->flush();
}

Status StarRocksFormatWriter::finish() {
    _tablet_writer->finish();
    if (_share_data) {
        return finish_txn_log();
    } else {
        return finish_schema_pb();
    }
}

StarRocksFormatChunk* StarRocksFormatWriter::new_chunk(size_t capacity) {
    auto schema = std::make_shared<starrocks::Schema>(ChunkHelper::convert_schema(_tablet_schema));
    StarRocksFormatChunk* format_chunk = new StarRocksFormatChunk(schema, capacity);
    return format_chunk;
}

Status StarRocksFormatWriter::finish_txn_log() {
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(_tablet_id);
    txn_log->set_txn_id(_txn_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : _tablet_writer->files()) {
        if (is_segment(f.path)) {
            op_write->mutable_rowset()->add_segments(std::move(f.path));
            op_write->mutable_rowset()->add_segment_size(f.size.value());
        } else if (is_del(f.path)) {
            op_write->add_dels(std::move(f.path));
        } else {
            return Status::InternalError(fmt::format("unknown file {}", f.path));
        }
    }
    op_write->mutable_rowset()->set_num_rows(_tablet_writer->num_rows());
    op_write->mutable_rowset()->set_data_size(_tablet_writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);
    return put_txn_log(std::move(txn_log));
}

Status StarRocksFormatWriter::finish_schema_pb() {
    std::string tablet_schema_path = _tablet_root_path + "/tablet.schema";

    if (_tablet_schema) {
        std::shared_ptr<TabletSchemaPB> pb = std::make_shared<TabletSchemaPB>();
        _tablet_schema->to_schema_pb(pb.get());
        auto fs_options = filter_map_by_key_prefix(_options, "fs.");
        ASSIGN_OR_RETURN(auto fs, FileSystem::Create(tablet_schema_path, FSOptions(fs_options)));
        size_t index = 0;
        string uuid = generate_uuid_string();
        for (auto& f : _tablet_writer->files()) {
            if (is_segment(f.path)) {
                string source = _tablet_root_path + "/data/" + f.path;
                string target = _tablet_root_path + "/data/" + uuid + "_" + std::to_string(index) + ".dat";
                std::cout << "AA = " << source << std::endl;
                std::cout << "AA = " << target << std::endl;
                RETURN_IF_ERROR(fs->rename_file(source, target));
                index++;

            } else {
                return Status::InternalError(fmt::format("unknown file {}", f.path));
            }
        }
        ProtobufFile file(tablet_schema_path, fs);
        return file.save(*pb);
    } else {
        return Status::InternalError("_tablet_schema was not defined");
    }
}
Status StarRocksFormatWriter::put_txn_log(const TxnLogPtr& log) {
    if (UNLIKELY(!log->has_tablet_id())) {
        return Status::InvalidArgument("txn log does not have tablet id");
    }
    if (UNLIKELY(!log->has_txn_id())) {
        return Status::InvalidArgument("txn log does not have txn id");
    }

    auto txn_log_path = _provider->txn_log_location(log->tablet_id(), log->txn_id());
    auto fs_options = filter_map_by_key_prefix(_options, "fs.");
    ASSIGN_OR_RETURN(auto fs, FileSystem::Create(txn_log_path, FSOptions(fs_options)));
    ProtobufFile file(txn_log_path, fs);
    return file.save(*log);

    return Status::OK();
}

StatusOr<TabletMetadataPtr> StarRocksFormatWriter::get_tablet_metadata(std::shared_ptr<FileSystem> fs) {
    std::vector<std::string> objects{};
    // TODO: construct prefix in LocationProvider
    std::string prefix = fmt::format("{:016X}_", _tablet_id);
    auto root = _provider->metadata_root_location(_tablet_id);

    auto scan_cb = [&](std::string_view name) {
        if (HasPrefixString(name, prefix)) {
            objects.emplace_back(join_path(root, name));
        }
        return true;
    };
    RETURN_IF_ERROR(fs->iterate_dir(root, scan_cb));

    if (objects.size() == 0) {
        return Status::NotFound(fmt::format("tablet {} metadata not found", _tablet_id));
    }
    std::sort(objects.begin(), objects.end());
    auto metadata_location = objects.back();
    return _lake_tablet_manager->get_tablet_metadata(fs, metadata_location, true);
}

} // namespace starrocks::lake::format
