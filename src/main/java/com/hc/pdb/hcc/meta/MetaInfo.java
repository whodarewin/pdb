package com.hc.pdb.hcc.meta;

public class MetaInfo {

    private long indexStartIndex;
    private long bloomStartIndex;

    public MetaInfo(long indexStartIndex, long bloomStartIndex) {
        this.indexStartIndex = indexStartIndex;
        this.bloomStartIndex = bloomStartIndex;
    }

    public long getIndexStartIndex() {
        return indexStartIndex;
    }

    public void setIndexStartIndex(long indexStartIndex) {
        this.indexStartIndex = indexStartIndex;
    }

    public long getBloomStartIndex() {
        return bloomStartIndex;
    }

    public void setBloomStartIndex(long bloomStartIndex) {
        this.bloomStartIndex = bloomStartIndex;
    }
}
