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

package com.starrocks.http.rest.v2.vo;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.UserException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class TabletView {

    @SerializedName("id")
    private Long id;

    @SerializedName("primaryComputeNodeId")
    private Long primaryComputeNodeId;

    @SerializedName("backendIds")
    private Set<Long> backendIds;

    @SerializedName("metaUrls")
    private List<String> metaUrls;

    public TabletView() {
    }

    /**
     * Create from {@link Tablet}
     */
    public static TabletView createFrom(Tablet tablet) {
        TabletView tvo = new TabletView();
        tvo.setId(tablet.getId());
        tvo.setBackendIds(tablet.getBackendIds());

        if (tablet instanceof LakeTablet) {
            LakeTablet lakeTablet = (LakeTablet) tablet;
            try {
                tvo.setPrimaryComputeNodeId(lakeTablet.getPrimaryComputeNodeId());
            } catch (UserException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        } else if (tablet instanceof LocalTablet) {
            List<String> metaUrls = new ArrayList<>();
            SystemInfoService infoService = GlobalStateMgr.getCurrentSystemInfo();
            for (long backendId : tablet.getBackendIds()) {
                Backend backend = infoService.getBackend(backendId);
                if (backend == null) {
                    continue;
                }
                String metaUrl = String.format("http://%s:%d/api/meta/header/%d",
                        backend.getHost(),
                        backend.getHttpPort(),
                        tablet.getId());
                metaUrls.add(metaUrl);
            }
            tvo.setMetaUrls(metaUrls);
        }

        return tvo;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getPrimaryComputeNodeId() {
        return primaryComputeNodeId;
    }

    public void setPrimaryComputeNodeId(Long primaryComputeNodeId) {
        this.primaryComputeNodeId = primaryComputeNodeId;
    }

    public Set<Long> getBackendIds() {
        return backendIds;
    }

    public void setBackendIds(Set<Long> backendIds) {
        this.backendIds = backendIds;
    }

    public List<String> getMetaUrls() {
        return metaUrls;
    }

    public void setMetaUrls(List<String> metaUrls) {
        this.metaUrls = metaUrls;
    }
}
