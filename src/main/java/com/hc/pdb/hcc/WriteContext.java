package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.util.ByteBloomFilter;

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
    private ByteBloomFilter bloom;

    public WriteContext(ByteBloomFilter bloom) {
        this.bloom = bloom;
    }

    public ByteArrayOutputStream getIndex() {
        return index;
    }

    public void setIndex(ByteArrayOutputStream index) {
        this.index = index;
    }

    public ByteBloomFilter getBloom() {
        return bloom;
    }

    public void setBloom(ByteBloomFilter bloom) {
        this.bloom = bloom;
    }
}
