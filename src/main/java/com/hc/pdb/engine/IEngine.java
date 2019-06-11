package com.hc.pdb.engine;


import java.io.IOException;

/**
 * IEngine
 * 引擎，包括lsm树，btree等
 * @author han.congcong
 * @date 2019/6/3
 */

public interface IEngine {
    /**
     * 写入
     * @param key   key
     * @param value value
     * @param ttl   过期时间
     */
    void put(byte[] key, byte[] value, long ttl) throws IOException;

    /**
     * 清空数据库
     */
    void clean();
}
