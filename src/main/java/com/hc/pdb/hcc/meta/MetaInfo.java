package com.hc.pdb.hcc.meta;

public class MetaInfo {

    private int indexStartIndex;
    private int bloomStartIndex;

    public MetaInfo(int indexStartIndex, int bloomStartIndex) {
        this.indexStartIndex = indexStartIndex;
        this.bloomStartIndex = bloomStartIndex;
    }

    public int getIndexStartIndex() {
        return indexStartIndex;
    }

    public void setIndexStartIndex(int indexStartIndex) {
        this.indexStartIndex = indexStartIndex;
    }

    public int getBloomStartIndex() {
        return bloomStartIndex;
    }

    public void setBloomStartIndex(int bloomStartIndex) {
        this.bloomStartIndex = bloomStartIndex;
    }
}
