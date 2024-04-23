package com.starrocks.format.rest.models;

import com.google.common.collect.Lists;

import java.util.List;

public class TabletCommitInfo {

    private long tabletId;
    private long backendId;

    // For low cardinality string column with global dict
    private List<String> invalidDictCacheColumns = Lists.newArrayList();
    private List<String> validDictCacheColumns = Lists.newArrayList();
    private List<Long> validDictCollectedVersions = Lists.newArrayList();

    public TabletCommitInfo(long tabletId, long backendId) {
        super();
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public TabletCommitInfo(long tabletId, long backendId, List<String> invalidDictCacheColumns,
                            List<String> validDictCacheColumns, List<Long> validDictCollectedVersions) {
        this.tabletId = tabletId;
        this.backendId = backendId;
        this.invalidDictCacheColumns = invalidDictCacheColumns;
        this.validDictCacheColumns = validDictCacheColumns;
        this.validDictCollectedVersions = validDictCollectedVersions;
    }

    public long getTabletId() {
        return tabletId;
    }

    public long getBackendId() {
        return backendId;
    }

    public List<String> getInvalidDictCacheColumns() {
        return invalidDictCacheColumns;
    }

    public List<String> getValidDictCacheColumns() {
        return validDictCacheColumns;
    }

    public List<Long> getValidDictCollectedVersions() {
        return validDictCollectedVersions;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(tabletId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TabletCommitInfo)) {
            return false;
        }

        TabletCommitInfo info = (TabletCommitInfo) obj;
        return (tabletId == info.tabletId) && (backendId == info.backendId);
    }
}
