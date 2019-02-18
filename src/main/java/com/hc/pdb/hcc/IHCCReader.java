package com.hc.pdb.hcc;

import com.hc.pdb.Cell;

import java.io.IOException;

public interface IHCCReader {
    /**
     * load bloom filter，index等信息
     */
    public static enum Result{
        not_exist,
        dontKnow
    }


    /**
     * 使用布隆过滤器判断是否存在于️这个hcc file中
     * @return
     */
    Result exist(byte[] key);

    /**
     * 下一个cell
     * @param key
     */
    Cell next(byte[] key) throws IOException;

    /**
     *  关闭
     */
    void close() throws IOException;
}
