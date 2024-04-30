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

#include "starrocks_format_reader.h"

#include "column/chunk.h"
#include "common/status.h"
#include "exec/olap_scan_prepare.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "format_utils.h"
#include "gen_cpp/QueryPlanExtra_types.h"
#include "runtime/descriptors.h"
#include "starrocks_format/starrocks_lib.h"
#include "storage/chunk_helper.h"
#include "storage/lake/fixed_location_provider.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/versioned_tablet.h"
#include "storage/olap_common.h"
#include "storage/predicate_parser.h"
#include "storage/projection_iterator.h"
#include "storage/protobuf_file.h"
#include "util/thrift_util.h"
#include "util/url_coding.h"

namespace starrocks::lake::format {

StarRocksFormatReader::StarRocksFormatReader(int64_t tablet_id, int64_t version,
                                             std::shared_ptr<TabletSchema>& required_tablet_schema,
                                             std::shared_ptr<TabletSchema>& output_tablet_schema,
                                             std::string tablet_root_path,
                                             std::unordered_map<std::string, std::string>& options)
        : _tablet_id(tablet_id),
          _version(version),
          _required_tablet_schema(required_tablet_schema),
          _output_tablet_schema(output_tablet_schema),
          _tablet_root_path(std::move(tablet_root_path)),
          _options(options) {
    auto it = _options.find("starrocks.format.chunk_size");
    if (it != _options.end() && !it->second.empty()) {
        _chunk_size = stoi(it->second);
    } else {
        _chunk_size = config::vector_chunk_size;
    }
}

Status StarRocksFormatReader::open() {
    LOG(INFO) << " Open tablet reader " << _tablet_id << " version: " << _version << " location: " << _tablet_root_path;
    // get tablet.
    // support the below file system options, same as hadoop aws fs options
    // fs.s3a.path.style.access default false
    // fs.s3a.access.key
    // fs.s3a.secret.key
    // fs.s3a.endpoint
    // fs.s3a.endpoint.region
    // fs.s3a.connection.ssl.enabled
    // fs.s3a.retry.limit
    // fs.s3a.retry.interval
    auto fs_options = filter_map_by_key_prefix(_options, "fs.");
    auto provider = std::make_shared<FixedLocationProvider>(_tablet_root_path);
    auto metadata_location = provider->tablet_metadata_location(_tablet_id, _version);
    ASSIGN_OR_RETURN(auto fs, FileSystem::Create(metadata_location, FSOptions(fs_options)));
    ASSIGN_OR_RETURN(auto metadata, _lake_tablet_manager->get_tablet_metadata(fs, metadata_location, true));

    // get tablet schema;
    _tablet_schema = std::make_shared<TabletSchema>(metadata->schema());
    bool using_column_uid = false;
    auto it = _options.find("starrocks.format.using_column_uid");
    if (it != _options.end() && !it->second.empty()) {
        using_column_uid = it->second.compare("true") == 0 ? true : false;
    }
    // get scan column index from tablet schema.
    std::vector<uint32_t> required_column_indexs;
    RETURN_IF_ERROR(schema_to_column_index(_required_tablet_schema, required_column_indexs, using_column_uid));
    // append key columns first.
    for (size_t i = 0; i < _tablet_schema->num_key_columns(); i++) {
        _scan_column_indexs.push_back(i);
    }
    for (auto index : required_column_indexs) {
        if (!_tablet_schema->column(index).is_key()) {
            _scan_column_indexs.push_back(index);
        }
    }
    std::sort(_scan_column_indexs.begin(), _scan_column_indexs.end());
    _scan_schema =
            std::make_shared<starrocks::Schema>(ChunkHelper::convert_schema(_tablet_schema, _scan_column_indexs));
    // create tablet reader
    _tablet = std::make_unique<VersionedTablet>(_lake_tablet_manager, metadata);
    ASSIGN_OR_RETURN(_tablet_reader, _tablet->new_reader(*_scan_schema));

    // get output column index from tablet schema
    std::vector<uint32_t> output_column_indexs;
    RETURN_IF_ERROR(schema_to_column_index(_output_tablet_schema, output_column_indexs, using_column_uid));
    // if scan columns not same as output column. we need project again after filter
    if (std::equal(_scan_column_indexs.begin(), _scan_column_indexs.end(), output_column_indexs.begin(),
                   output_column_indexs.end())) {
        _output_schema = _scan_schema;
    } else {
        _need_project = true;
        _output_schema =
                std::make_shared<starrocks::Schema>(ChunkHelper::convert_schema(_tablet_schema, output_column_indexs));
        LOG(INFO) << "OutputSchema and ScanSchema are not same. OutputSchema size is " << output_column_indexs.size()
                  << " and ScanSchema size is " << _scan_column_indexs.size() << ".";
        RETURN_IF_ERROR(build_output_index_map(_output_schema, _scan_schema));
    }

    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_BYPASS_QUERY;
    read_params.skip_aggregation = false;
    read_params.chunk_size = _chunk_size;
    read_params.use_page_cache = false;
    read_params.lake_io_opts.fill_data_cache = false;
    read_params.lake_io_opts.fs = fs;
    read_params.lake_io_opts.location_provider = provider;
    auto query_plan_iter = _options.find("starrocks.format.query_plan");
    if (query_plan_iter != _options.end() && !query_plan_iter->second.empty()) {
        _state = std::make_shared<RuntimeState>();
        RETURN_IF_ERROR(parse_query_plan(query_plan_iter->second));
        RETURN_IF_ERROR(init_reader_params(read_params));
    }

    RETURN_IF_ERROR(_tablet_reader->prepare());
    RETURN_IF_ERROR(_tablet_reader->open(read_params));

    return Status::OK();
}

Status StarRocksFormatReader::build_output_index_map(const std::shared_ptr<starrocks::Schema>& output,
                                                     const std::shared_ptr<starrocks::Schema>& input) {
    DCHECK(output);
    DCHECK(input);
    std::unordered_map<ColumnId, size_t> input_indexes;
    for (size_t i = 0; i < input->num_fields(); i++) {
        input_indexes[input->field(i)->id()] = i;
    }

    _index_map.resize(output->num_fields());
    for (size_t i = 0; i < output->num_fields(); i++) {
        if (input_indexes.count(output->field(i)->id()) == 0) {
            std::stringstream ss;
            ss << "Output column(" << output->field(i)->name() << ") is not in scan column list!";
            LOG(WARNING) << ss.str();
            return Status::InvalidArgument(ss.str());
        }
        _index_map[i] = input_indexes[output->field(i)->id()];
    }
    return Status::OK();
}

Status StarRocksFormatReader::schema_to_column_index(std::shared_ptr<TabletSchema>& tablet_part_schema,
                                                     std::vector<uint32_t>& column_indexs, bool using_column_uid) {
    std::stringstream ss;
    for (int col_idx = 0; col_idx < tablet_part_schema->num_columns(); col_idx++) {
        int32_t index = 0;
        if (using_column_uid) {
            index = _tablet_schema->field_index(tablet_part_schema->column(col_idx).unique_id());
            if (index < 0) {
                ss << "invalid field unique id: " << tablet_part_schema->column(col_idx).unique_id();
            }
        } else {
            index = _tablet_schema->field_index(tablet_part_schema->column(col_idx).name());
            if (index < 0) {
                ss << "invalid field name: " << tablet_part_schema->column(col_idx).name();
            }
        }
        if (index < 0) {
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        column_indexs.push_back(index);
    }
    return Status::OK();
}

void StarRocksFormatReader::close() {
    if (_tablet_reader) {
        // log statistics
        LOG(INFO) << "Close tablet reader with "
                  << " bytes_read: " << _tablet_reader->stats().bytes_read
                  << ", compressed_bytes_read_request: " << _tablet_reader->stats().compressed_bytes_read_request
                  << ", io_count_request: " << _tablet_reader->stats().io_count_request
                  << ", late_materialize_ns: " << _tablet_reader->stats().late_materialize_ns
                  << ", io_ns : " << _tablet_reader->stats().io_ns;
        // close reader to update statistics before update counters
        _tablet_reader->close();
    }

    if (_tablet_reader) {
        _tablet_reader.reset();
    }
    _predicate_free_pool.clear();
}

Status StarRocksFormatReader::parse_query_plan(std::string& encoded_query_plan) {
    std::string query_plan_info;
    if (!base64_decode(encoded_query_plan, &query_plan_info)) {
        LOG(WARNING) << "Open reader error: decode query_plan failure";
        std::stringstream msg;
        msg << "Open reader error: invalidate query_plan" << encoded_query_plan;
        return Status::InvalidArgument(msg.str());
    }
    const auto* buf = (const uint8_t*)query_plan_info.data();
    uint32_t len = query_plan_info.size();

    // deserialize TQueryPlanInfo
    TQueryPlanInfo t_query_plan_info;
    RETURN_IF_ERROR(deserialize_thrift_msg(buf, &len, TProtocolType::BINARY, &t_query_plan_info));
    // LOG(INFO) << " query plan " << t_query_plan_info;
    starrocks::TPlanNode* plan_node = nullptr;
    for (auto& node : t_query_plan_info.plan_fragment.plan.nodes) {
        if (node.node_type == starrocks::TPlanNodeType::LAKE_SCAN_NODE) {
            if (!plan_node) {
                plan_node = &node;
            } else {
                return Status::InvalidArgument("There should be only one lake scan node in query plan!");
            }
        }
    }

    // There should be a lake scan plan node, because only one table in query plan.
    if (!plan_node) {
        return Status::InvalidArgument("There is no lake scan node in query plan!");
    }

    // get tuple descriptor
    RETURN_IF_ERROR(DescriptorTbl::create(_state.get(), &_obj_pool, t_query_plan_info.desc_tbl, &_desc_tbl, 4096));
    auto tuple_id = plan_node->lake_scan_node.tuple_id;
    _tuple_desc = _desc_tbl->get_tuple_descriptor(tuple_id);
    for (auto slot : _tuple_desc->slots()) {
        DCHECK(slot->is_materialized());
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "invalid field name: " << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        // set query slots for pushdown filter
        auto it = std::find(_scan_column_indexs.begin(), _scan_column_indexs.end(), index);
        if (it != _scan_column_indexs.end()) {
            _query_slots.push_back(slot);
        }
    }
    // get conjuncts
    if (plan_node->__isset.conjuncts && plan_node->conjuncts.size() > 0) {
        RETURN_IF_ERROR(Expr::create_expr_trees(&_obj_pool, plan_node->conjuncts, &_conjunct_ctxs, _state.get()));
        RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, _state.get()));
        RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, _state.get()));
    }

    return Status::OK();
}

Status StarRocksFormatReader::init_reader_params(TabletReaderParams& params) {
    _conjuncts_manager.conjunct_ctxs_ptr = &_conjunct_ctxs;
    _conjuncts_manager.tuple_desc = _tuple_desc;
    _conjuncts_manager.obj_pool = &_obj_pool;
    _conjuncts_manager.runtime_filters = nullptr;
    auto key_column_names = std::make_shared<std::vector<std::string>>();
    if (KeysType::PRIMARY_KEYS == _tablet_schema->keys_type() && _tablet_schema->sort_key_idxes().size() > 0) {
        for (auto sort_key_index : _tablet_schema->sort_key_idxes()) {
            TabletColumn col = _tablet_schema->column(sort_key_index);
            key_column_names->push_back(std::string(col.name()));
        }
    } else {
        for (auto col : _tablet_schema->columns()) {
            if (col.is_key()) {
                key_column_names->push_back(std::string(col.name()));
            }
        }
    }
    _conjuncts_manager.key_column_names = key_column_names.get();

    _conjuncts_manager.runtime_state = _state.get();
    bool enable_column_expr_predicate = false;
    RETURN_IF_ERROR(_conjuncts_manager.parse_conjuncts(true, config::max_scan_key_num, enable_column_expr_predicate));

    auto parser = _obj_pool.add(new PredicateParser(_tablet_schema));
    std::vector<PredicatePtr> preds;
    RETURN_IF_ERROR(_conjuncts_manager.get_column_predicates(parser, &preds));
    // decide_chunk_size(!preds.empty());
    // _has_any_predicate = (!preds.empty());
    for (auto& p : preds) {
        if (parser->can_pushdown(p.get())) {
            params.predicates.push_back(p.get());
        } else {
            _not_push_down_predicates.add(p.get());
        }
        _predicate_free_pool.emplace_back(std::move(p));
    }

    _conjuncts_manager.get_not_push_down_conjuncts(&_not_push_down_conjuncts);
    // RETURN_IF_ERROR(_dict_optimize_parser.rewrite_conjuncts(&_not_push_down_conjuncts, _state));

    std::vector<std::unique_ptr<OlapScanRange>> key_ranges;
    RETURN_IF_ERROR(_conjuncts_manager.get_key_ranges(&key_ranges));

    std::vector<OlapScanRange*> scanner_ranges;
    int scanners_per_tablet = 64;
    int num_ranges = key_ranges.size();
    int ranges_per_scanner = std::max(1, num_ranges / scanners_per_tablet);
    for (int i = 0; i < num_ranges;) {
        scanner_ranges.push_back(key_ranges[i].get());
        i++;
        for (int j = 1;
             i < num_ranges && j < ranges_per_scanner && key_ranges[i]->end_include == key_ranges[i - 1]->end_include;
             ++j, ++i) {
            scanner_ranges.push_back(key_ranges[i].get());
        }
    }

    for (const auto& key_range : scanner_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }
        params.range = key_range->begin_include ? TabletReaderParams::RangeStartOperation::GE
                                                : TabletReaderParams::RangeStartOperation::GT;
        params.end_range = key_range->end_include ? TabletReaderParams::RangeEndOperation::LE
                                                  : TabletReaderParams::RangeEndOperation::LT;

        params.start_key.push_back(key_range->begin_scan_range);
        params.end_key.push_back(key_range->end_scan_range);
    }

    return Status::OK();
}

StarRocksFormatChunk* StarRocksFormatReader::get_next() {
    auto chunk = ChunkHelper::new_chunk(*_output_schema, _chunk_size);
    Status status = do_get_next(chunk);

    if (status.ok()) {
        StarRocksFormatChunk* format_chunk = new StarRocksFormatChunk(std::move(chunk));
        return format_chunk;
    } else if (status.is_end_of_file()) {
        LOG(INFO) << "no more data in tablet " << _tablet_id;
        StarRocksFormatChunk* format_chunk = new StarRocksFormatChunk(_output_schema, 0);
        return format_chunk;
    } else {
        LOG(ERROR) << "get_next failed! " << status.message();
        return nullptr;
    }
}
Status StarRocksFormatReader::do_get_next(ChunkUniquePtr& chunk_ptr) {
    auto* output_chunk = chunk_ptr.get();
    if (!_scan_chunk) {
        _scan_chunk = ChunkHelper::new_chunk(_tablet_reader->output_schema(), _chunk_size);
    }
    _scan_chunk->reset();

    do {
        RETURN_IF_ERROR(_tablet_reader->get_next(_scan_chunk.get()));
        // If there is no filter, _query_slots will be empty.
        for (auto slot : _query_slots) {
            size_t column_index = _scan_chunk->schema()->get_field_index_by_name(slot->col_name());
            _scan_chunk->set_slot_id_to_index(slot->id(), column_index);
        }

        if (!_not_push_down_predicates.empty()) {
            // SCOPED_TIMER(_expr_filter_timer);
            size_t nrows = _scan_chunk->num_rows();
            _selection.clear();
            _selection.resize(nrows);
            _not_push_down_predicates.evaluate(_scan_chunk.get(), _selection.data(), 0, nrows);
            _scan_chunk->filter(_selection);
            DCHECK_CHUNK(_scan_chunk);
        }
        if (!_not_push_down_conjuncts.empty()) {
            // SCOPED_TIMER(_expr_filter_timer);
            auto status = ExecNode::eval_conjuncts(_not_push_down_conjuncts, _scan_chunk.get());
            DCHECK_CHUNK(_scan_chunk.get());
        }
        if (_need_project) {
            Columns& input_columns = _scan_chunk->columns();
            for (size_t i = 0; i < _index_map.size(); i++) {
                output_chunk->get_column_by_index(i).swap(input_columns[_index_map[i]]);
            }
        } else {
            auto scan_chunk = _scan_chunk.get();
            output_chunk->swap_chunk(*(scan_chunk));
        }
    } while (output_chunk->num_rows() == 0);

    return Status::OK();
}

} // namespace starrocks::lake::format
