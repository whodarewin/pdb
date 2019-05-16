package com.hc.pdb.engine;

/**
 * 引擎，包括lsm树，btree等
 */
public interface IEngine {
    /**
     * 写入
     * @param key key
     * @param value value
     * @param ttl 过期时间
     */
    void put(byte[] key, byte[] value, long ttl);
}
