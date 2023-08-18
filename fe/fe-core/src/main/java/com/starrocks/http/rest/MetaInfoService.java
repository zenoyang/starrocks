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

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Get meta info with privilege checking
 */
public class MetaInfoService {

    public static class GetDatabasesAction extends RestBaseAction {

        public GetDatabasesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetDatabasesAction action = new GetDatabasesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/meta/_databases", action);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            Pair<Integer, Integer> fromToIndex = getFromToIndexByParameter(request);
            List<String> dbNames = GlobalStateMgr.getCurrentState().getDbNames();
            Map<String, Object> resultMap = new HashMap<>(4);
            try {
                List<String> resultDbNameSet = dbNames.stream()
                        .map(fullName -> {
                            final String db = ClusterNamespace.getNameFromFullName(fullName);
                            try {
                                Authorizer.checkAnyActionOnOrInDb(ConnectContext.get().getCurrentUserIdentity(),
                                        ConnectContext.get().getCurrentRoleIds(),
                                        InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, db);
                            } catch (Exception e) {
                                return null;
                            }
                            return db;
                        })
                        .filter(Objects::nonNull)
                        .sorted()
                        .skip(fromToIndex.first)
                        .limit(fromToIndex.second)
                        .collect(Collectors.toList());
                resultMap.put("status", 200);
                resultMap.put("databases", resultDbNameSet);
                resultMap.put("count", resultDbNameSet.size());
            } catch (StarRocksHttpException e) {
                resultMap.put("status", e.getCode().code());
                resultMap.put("exception", e.getMessage());
            }

            try {
                ObjectMapper mapper = new ObjectMapper();
                String result = mapper.writeValueAsString(resultMap);
                response.setContentType("application/json");
                response.getContent().append(result);
                sendResult(request, response,
                        HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
            } catch (Exception e) {
                response.getContent().append(e.getMessage());
                sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }
    }

    public static class GetTablesAction extends RestBaseAction {

        public GetTablesAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            GetTablesAction action = new GetTablesAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/meta/{" + DB_KEY + "}/_tables", action);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
            String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            String dbName = request.getSingleParameter(DB_KEY);
            Pair<Integer, Integer> fromToIndex = getFromToIndexByParameter(request);

            Map<String, Object> resultMap = new HashMap<>(4);

            try {
                if (Strings.isNullOrEmpty(dbName)) {
                    throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "{database} must be selected");
                }
                try {
                    Authorizer.checkAnyActionOnOrInDb(ConnectContext.get().getCurrentUserIdentity(), ConnectContext.get()
                            .getCurrentRoleIds(), catalogName, dbName);
                } catch (AccessDeniedException e) {
                    throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN,
                            "List tables in [" + dbName + "] " + " failed.");
                }
                Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
                if (db == null) {
                    throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                            "Database [" + dbName + "] " + "does not exists");
                }
                List<String> resultTblNames = db.getTables().stream()
                        .map(table -> {
                            try {
                                Authorizer.checkAnyActionOnTable(ConnectContext.get().getCurrentUserIdentity(),
                                        ConnectContext.get().getCurrentRoleIds(),
                                        new TableName(db.getFullName(), table.getName()));
                            } catch (AccessDeniedException e) {
                                return null;
                            }
                            return table.getName();
                        })
                        .filter(Objects::nonNull)
                        .skip(fromToIndex.first)
                        .limit(fromToIndex.second)
                        .collect(Collectors.toList());

                // handle limit offset
                resultMap.put("status", 200);
                resultMap.put("database", dbName);
                resultMap.put("tables", resultTblNames);
                resultMap.put("table count", resultTblNames.size());
            } catch (StarRocksHttpException e) {
                resultMap.put("status", e.getCode().code());
                resultMap.put("exception", e.getMessage());
            }

            ObjectMapper mapper = new ObjectMapper();
            try {
                String result = mapper.writeValueAsString(resultMap);
                response.setContentType("application/json");
                response.getContent().append(result);
                sendResult(request, response,
                        HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
            } catch (Exception e) {
                response.getContent().append(e.getMessage());
                sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
            }
        }

    }

    // get limit and offset from query parameter
    // and return fromIndex and toIndex of a list
    private static Pair<Integer, Integer> getFromToIndexByParameter(BaseRequest request) {
        String limitStr = request.getSingleParameter("limit");
        String offsetStr = request.getSingleParameter("offset");

        int offset = 0;
        int limit = Integer.MAX_VALUE;

        limit = (limitStr != null && !limitStr.isEmpty()) ? Integer.parseInt(limitStr) : limit;
        offset = (offsetStr != null && !offsetStr.isEmpty()) ? Integer.parseInt(offsetStr) : offset;

        if (limit < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param limit should >= 0");
        }
        if (offset < 0) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "Param offset should >= 0");
        }
        if (offset > 0 && limit == Integer.MAX_VALUE) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST,
                    "Param offset should be set with param limit");
        }
        return Pair.create(offset, limit + offset);
    }

    private static Pair<Integer, Integer> getFromToIndex(Pair<Integer, Integer> offsetAndLimit, int maxNum) {
        if (maxNum <= 0) {
            return Pair.create(0, 0);
        }
        return Pair.create(Math.min(offsetAndLimit.first, maxNum - 1), Math.min(offsetAndLimit.second, maxNum));
    }
}
