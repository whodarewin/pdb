package com.hc.pdb.hcc;

/**
 * HCC file的meta信息
 */
public class HCCMeta {

    private long bloomIndex;//布隆过滤器开始的index
    private long indexIndex;//索引开始的index
    private long blockIndex;//block开始的index

    public long getBloomIndex() {
        return bloomIndex;
    }

    public void setBloomIndex(long bloomIndex) {
        this.bloomIndex = bloomIndex;
    }

    public long getIndexIndex() {
        return indexIndex;
    }

    public void setIndexIndex(long indexIndex) {
        this.indexIndex = indexIndex;
    }

    public long getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(long blockIndex) {
        this.blockIndex = blockIndex;
    }
}
