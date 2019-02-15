package com.hc.pdb.hcc;

public interface IHCCReader {
    /**
     * load bloom filter，index等信息
     */
    public static enum Result{
        exit,
        dontKnow
    }


    /**
     * 使用布隆过滤器判断是否存在于️这个hcc file中
     * @return
     */
    Result exist(byte[] key);

    /**
     * 下一个key
     * @param key
     */
    void next(byte[] key);

    /**
     *  关闭
     */
    void close();
}
