package com.hc.pdb.hcc;

import com.hc.pdb.Cell;

import java.io.IOException;

public interface IHCCReader {
    /**
     * 使用布隆过滤器判断是否存在于️这个hcc file中
     *
     * @return
     */
    Result exist(byte[] key);

    /**
     * seek 到某一位置
     * @param key
     */
    void seek(byte[] key) throws IOException;

    /**
     * 下一个cell
     */
    Cell next() throws IOException;

    /**
     * 关闭
     */
    void close() throws IOException;

    /**
     * load bloom filter，index等信息
     */
    enum Result {
        /**
         * 不存在
         */
        not_exist,
        /**
         * 不清楚是否存在
         */
        dontKnow
    }
}
