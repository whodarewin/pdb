package com.hc.pdb.wal;

import com.hc.pdb.ISerializable;

import java.io.IOException;

/**
 * IWalWriter
 * 前缀：cell：cell。。。。
 * 提供三种flush方式，对应操作系统刷新页到磁盘的三种方式
 * @author kq
 * @date 2019/6/10
 */
public interface IWalWriter {

    void write(ISerializable serializable) throws IOException;

    void close() throws IOException;
}
