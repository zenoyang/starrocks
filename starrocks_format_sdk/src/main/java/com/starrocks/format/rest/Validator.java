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

package com.starrocks.format.rest;

import com.google.common.base.Strings;
import com.starrocks.format.rest.model.TableSchema;

import java.util.Map;

public class Validator {

    public static boolean validateSegmentLoadExport(TableSchema tableSchema) throws LoadNonSupportException {
        // 0. only olap table support segment load
        if (!tableSchema.getTableType().equalsIgnoreCase("OLAP")) {
            throw new LoadNonSupportException("Only olap table support for segment load.");
        }

        // 1. only normal table state support segment load
        if (!tableSchema.getState().equalsIgnoreCase("Normal")) {
            String state = tableSchema.getState();
            throw new LoadNonSupportException("Table is in " + state + " state, it did not support for segment load.");
        }

        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            // 2.1 large int
            if (tableSchema.getColumns().get(i).getPrimitiveType().equalsIgnoreCase("LargeInt")) {
                throw new LoadNonSupportException("Column type: LargeInt was not support for segment load.");
            }

            // 2.2 auto increment column
            if (tableSchema.getColumns().get(i).getAutoIncrement()) {
                throw new LoadNonSupportException("Auto increment column was not support for segment load.");
            }
        }

        // 3. expr partition. But the "Partitioning based on the column expression" was list partition type. Now
        // there are no way to classify it.
        String partitionType = tableSchema.getPartitionInfo().getType();
        if (partitionType.equalsIgnoreCase("EXPR_RANGE") || partitionType.equalsIgnoreCase("EXPR_RANGE_V2")) {
            throw new LoadNonSupportException("Expr range partition was not support for segment load.");
        }

        // 4. rollup
        if (tableSchema.getIndexMetas().size() > 1) {
            throw new LoadNonSupportException("Rollup was not support for segment load.");
        }

        // 5. colocate table
        if (!Strings.isNullOrEmpty(tableSchema.getColocateGroup())) {
            throw new LoadNonSupportException("Colocate group was not support for segment load.");
        }

        Map<String, String> properties = tableSchema.getProperties();

        // 6. hybrid row colum table
        if (properties.containsKey("storage_type")
                && properties.get("storage_type").equalsIgnoreCase("column_with_row")) {
            throw new LoadNonSupportException("Hybrid row-column storage was not support for segment load.");
        }

        // 7. unique_constraints
        if (!Strings.isNullOrEmpty(properties.get("unique_constraints"))) {
            throw new LoadNonSupportException("Unique constraints was not support for segment load.");
        }

        // 8. foreign_key_constraints
        if (!Strings.isNullOrEmpty(properties.get("foreign_key_constraints"))) {
            throw new LoadNonSupportException("Foreign key constraints was not support for segment load.");
        }

        // 9. bloom filter columns
        if (tableSchema.getBfColumns() != null && !tableSchema.getBfColumns().isEmpty()) {
            throw new LoadNonSupportException("Bloom filter columns was not support for segment load.");
        }

        // 10. bitmap index
        for (int i = 0; i < tableSchema.getIndexes().size(); i++) {
            if (tableSchema.getIndexes().get(i).getIndexType().equalsIgnoreCase("BITMAP")) {
                throw new LoadNonSupportException("Bitmap index was not support for segment load.");
            }
        }

        return true;
    }
}
