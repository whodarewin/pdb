package com.hc.pdb.engine;


import com.hc.pdb.scanner.IScanner;

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
     * 删除
     * @param key 需要删除的cell的key
     */
    void delete(byte[] key) throws IOException;

    /**
     * scan 检索方式
     * @param start start key
     * @param end end key
     * @return
     */
    IScanner scan(byte[] start, byte[] end);

    /**
     * get 操作
     * @param key 需要get的key
     * @return 返回key所对应的值
     */
    byte[] get(byte[] key);
    /**
     * 清空数据库
     */
    void clean();
}
