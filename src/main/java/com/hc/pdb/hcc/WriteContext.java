package com.hc.pdb.hcc;

import java.io.ByteArrayOutputStream;

/**
 * 写hcc时的上下文，用于收集关键position，block，index等。
 */
public class WriteContext {
    /**
     * 索引,格式：startkeyLength,startKey,start index in long
     */
    private ByteArrayOutputStream index = new ByteArrayOutputStream();

    /**
     * 布隆过滤器
     */
    private ByteArrayOutputStream bloom = new ByteArrayOutputStream();

    public ByteArrayOutputStream getIndex() {
        return index;
    }

    public void setIndex(ByteArrayOutputStream index) {
        this.index = index;
    }

    public ByteArrayOutputStream getBloom() {
        return bloom;
    }

    public void setBloom(ByteArrayOutputStream bloom) {
        this.bloom = bloom;
    }
}
