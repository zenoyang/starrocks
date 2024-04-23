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

#include "starrocks_format_chunk.h"

#include "storage/lake/tablet_reader.h"
#include "storage/tablet_schema.h"

#include "exec/olap_scan_prepare.h"
#include "storage/column_predicate.h"

namespace starrocks::lake::format {

class StarRocksFormatReader {
public:
    StarRocksFormatReader() = default;
    StarRocksFormatReader(int64_t tablet_id, int64_t version, std::shared_ptr<TabletSchema>& required_tablet_schema,
                          std::shared_ptr<TabletSchema>& output_tablet_schema, std::string tablet_root_path,
                          std::unordered_map<std::string, std::string>& options);

    StarRocksFormatReader(StarRocksFormatReader&&) = default;
    StarRocksFormatReader& operator=(StarRocksFormatReader&&) = default;

    int64_t tablet_id() { return _tablet_id; };
    const std::string tablet_root_path() { return _tablet_root_path; };

    Status open();
    void close();

    Status parse_query_plan(std::string& encoded_query_plan);
    Status init_reader_params(TabletReaderParams& params);

    StarRocksFormatChunk* get_next();
    Status do_get_next(ChunkUniquePtr& chunk_ptr);

private:
    bool need_project_after_filter();
    Status schema_column_name_to_id(std::shared_ptr<TabletSchema>& tablet_schema, std::vector<uint32_t>& column_indexs,
                                    bool using_column_uid);

private:
    int64_t _tablet_id;
    int64_t _version;
    std::shared_ptr<TabletSchema> _required_tablet_schema;
    std::shared_ptr<TabletSchema> _output_tablet_schema;
    starrocks::Schema _output_schema;
    std::string _tablet_root_path;
    std::unordered_map<std::string, std::string> _options;

    int32_t _chunk_size;
    std::unique_ptr<VersionedTablet> _tablet;
    std::shared_ptr<TabletReader> _tablet_reader;
    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;
    std::shared_ptr<ChunkIterator> _output_iter;
    std::shared_ptr<RuntimeState> _state;

    //
    std::shared_ptr<TabletSchema> _tablet_schema;
    // starrocks::Schema _required_schema;
    std::unordered_set<uint32_t> _unused_output_column_ids;
    std::vector<uint32_t> _required_column_indexs;
    std::vector<uint32_t> _output_column_indexs;

    //
    ObjectPool _obj_pool;
    // _desc_tbl, tuple_desc,  _query_slots, _conjunct_ctxs memory are maintained by _obj_pool
    DescriptorTbl* _desc_tbl = nullptr;
    TupleDescriptor* _tuple_desc = nullptr;
    // slot descriptors for each one of |output_columns|. used by _not_push_down_conjuncts.
    std::vector<SlotDescriptor*> _query_slots;
    std::vector<ExprContext*> _conjunct_ctxs;

    ///
    OlapScanConjunctsManager _conjuncts_manager;
    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    // The conjuncts couldn't push down to storage engine
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    ConjunctivePredicates _not_push_down_predicates;
    std::vector<PredicatePtr> _predicate_free_pool;
    std::vector<uint8_t> _selection;

    // need choose select columns after filter
    bool _need_project_after_filter = false;
};

} // namespace starrocks::lake::format
