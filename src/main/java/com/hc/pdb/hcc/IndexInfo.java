package com.hc.pdb.hcc;

/**
 * block 的index
 */
public class IndexInfo {
    private long index;
    private byte[] key;

    public IndexInfo(long index, byte[] key) {
        this.index = index;
        this.key = key;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}
