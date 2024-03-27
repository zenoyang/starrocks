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

package com.starrocks.http.rest;

import com.google.common.collect.Maps;
import com.starrocks.backup.Repository;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.model.TableDetailDto;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableDetailAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(Repository.class);

    private static final String PARAM_WITH_MV = "with_mv";
    private static final String PARAM_WITH_PROPERTY = "with_property";

    public TableDetailAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/meta/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_detail",
                new TableDetailAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        String withMvPara = request.getSingleParameter(PARAM_WITH_MV);
        boolean withMv = isParameterTrue(withMvPara);
        String withPropertyPara = request.getSingleParameter(PARAM_WITH_PROPERTY);
        boolean withProperty = isParameterTrue(withPropertyPara);
        try {
            Optional.ofNullable(dbName).orElseThrow(
                    () -> new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "No database selected."));
            Optional.ofNullable(tableName).orElseThrow(
                    () -> new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "No table selected."));
            // check privilege for select, otherwise return 403 HTTP status
            try {
                Authorizer.checkTableAction(ConnectContext.get().getCurrentUserIdentity(),
                        ConnectContext.get().getCurrentRoleIds(), dbName, tableName, PrivilegeType.SELECT);
            } catch (AccessDeniedException e) {
                throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN,
                        "Get table [" + tableName + "] " + " detail failed.");
            }

            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Database [" + dbName + "] " + "does not exists");
            }
            db.readLock();
            try {
                Table table = db.getTable(tableName);
                if (table == null) {
                    throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                            "Table [" + tableName + "] " + "does not exists");
                }

                TableDetailDto.TableSchemaInfoDto tableSchemaInfoDto = new TableDetailDto.TableSchemaInfoDto();
                tableSchemaInfoDto.setEngineType(table.getType().toString());

                TableDetailDto.SchemaInfoDto schemaInfo = generateSchemaInfo(table, withMv);
                tableSchemaInfoDto.setSchemaInfo(schemaInfo);

                fillMoreOlapMetaInfo(table, tableSchemaInfoDto, withProperty);

                resultMap.put("status", 200);
                resultMap.put("table", tableSchemaInfoDto);
            } finally {
                db.readUnlock();
            }
        } catch (StarRocksHttpException e) {
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        } catch (Exception e) {
            resultMap.put("status", HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
            resultMap.put("exception", e.getMessage() == null ? "Null Pointer Exception" : e.getMessage());
        }

        ObjectMapper mapper = new ObjectMapper();
        try {
            String result = mapper.writeValueAsString(resultMap);
            // send result with extra information
            response.setContentType("application/json");
            response.getContent().append(result);
            sendResult(request, response,
                    HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
        } catch (Exception e) {
            // may be this never happen
            response.getContent().append(e.getMessage());
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private TableDetailDto.SchemaInfoDto generateSchemaInfo(Table tbl, boolean withMv) {
        TableDetailDto.SchemaInfoDto schemaInfoDto = new TableDetailDto.SchemaInfoDto();
        Map<String, TableDetailDto.TableSchemaDto> schemaMap = Maps.newHashMap();
        if (tbl.isOlapOrCloudNativeTable()) {
            OlapTable olapTable = (OlapTable) tbl;
            long baseIndexId = olapTable.getBaseIndexId();
            TableDetailDto.TableSchemaDto baseTableSchemaDto = new TableDetailDto.TableSchemaDto();
            baseTableSchemaDto.setBaseIndex(true);
            baseTableSchemaDto.setKeyType(olapTable.getKeysTypeByIndexId(baseIndexId).name());
            List<TableDetailDto.SchemaDto> baseSchema = generateSchema(olapTable.getSchemaByIndexId(baseIndexId));
            baseTableSchemaDto.setSchemaList(baseSchema);
            schemaMap.put(olapTable.getIndexNameById(baseIndexId), baseTableSchemaDto);

            if (withMv) {
                for (long indexId : olapTable.getIndexIdListExceptBaseIndex()) {
                    TableDetailDto.TableSchemaDto tableSchemaDto = new TableDetailDto.TableSchemaDto();
                    tableSchemaDto.setBaseIndex(false);
                    tableSchemaDto.setKeyType(olapTable.getKeysTypeByIndexId(indexId).name());
                    List<TableDetailDto.SchemaDto> schema = generateSchema(olapTable.getSchemaByIndexId(indexId));
                    tableSchemaDto.setSchemaList(schema);
                    schemaMap.put(olapTable.getIndexNameById(indexId), tableSchemaDto);
                }
            }

        } else {
            TableDetailDto.TableSchemaDto tableSchemaDto = new TableDetailDto.TableSchemaDto();
            tableSchemaDto.setBaseIndex(false);
            List<TableDetailDto.SchemaDto> schema = generateSchema(tbl.getBaseSchema());
            tableSchemaDto.setSchemaList(schema);
            schemaMap.put(tbl.getName(), tableSchemaDto);
        }
        schemaInfoDto.setSchemaMap(schemaMap);
        return schemaInfoDto;
    }

    private List<TableDetailDto.SchemaDto> generateSchema(List<Column> columns) {
        return columns.stream().map(column -> {
            TableDetailDto.SchemaDto schemaDto = new TableDetailDto.SchemaDto();
            schemaDto.setField(column.getName());
            schemaDto.setType(column.getType().canonicalName());
            schemaDto.setIsNull(String.valueOf(column.isAllowNull()));
            schemaDto.setDefaultVal(column.getDefaultValue());
            schemaDto.setKey(String.valueOf(column.isKey()));
            schemaDto.setAggrType(column.getAggregationType() == null
                    ? "None" : column.getAggregationType().toString());
            schemaDto.setComment(column.getComment());
            return schemaDto;
        }).collect(Collectors.toList());
    }

    private void fillMoreOlapMetaInfo(Table table, TableDetailDto.TableSchemaInfoDto tableSchemaInfoDto,
                                      boolean withProperty) {
        if (table.getType() == Table.TableType.OLAP) {
            OlapTable olapTable = (OlapTable) table;
            fillTableProperties(tableSchemaInfoDto, withProperty, olapTable.getTableProperty());
            fillPartitionInfo(tableSchemaInfoDto, olapTable.getPartitionInfo());
            fillDistributionInfo(tableSchemaInfoDto, olapTable.getDefaultDistributionInfo());
        }
    }

    private void fillTableProperties(TableDetailDto.TableSchemaInfoDto tableSchemaInfoDto, boolean withProperty,
                                     TableProperty tableProperty) {
        if (withProperty && tableProperty != null) {
            tableSchemaInfoDto.setProperties(tableProperty.getProperties());
        }
    }

    private void fillPartitionInfo(TableDetailDto.TableSchemaInfoDto tableSchemaInfoDto, PartitionInfo partitionInfo) {
        TableDetailDto.PartitionInfoDto partitionInfoDto = new TableDetailDto.PartitionInfoDto();
        partitionInfoDto.setPartitionType(partitionInfo.getType().toString());
        partitionInfoDto.setPartitionColumns(
                partitionInfo.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toList()));
        tableSchemaInfoDto.setPartitionInfo(partitionInfoDto);
    }

    private void fillDistributionInfo(TableDetailDto.TableSchemaInfoDto tableSchemaInfoDto,
                                      DistributionInfo distributionInfo) {
        TableDetailDto.DistributionInfoDto distributionInfoDto = new TableDetailDto.DistributionInfoDto();
        distributionInfoDto.setDistributionInfoType(distributionInfo.getType().toString());

        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            distributionInfoDto.setDistributionColumns(hashDistributionInfo.getDistributionColumns().stream()
                    .map(Column::getName).collect(Collectors.toList()));
            distributionInfoDto.setBucketNum(distributionInfo.getBucketNum());
        } else if (distributionInfo instanceof RandomDistributionInfo) {
            distributionInfoDto.setBucketNum(distributionInfo.getBucketNum());
        }
        tableSchemaInfoDto.setDistributionInfo(distributionInfoDto);
    }

    private static boolean isParameterTrue(String parameter) {
        return "1".equals(parameter);
    }

}
