package com.hc.pdb.wal;

import com.hc.pdb.Cell;

/**
 * IWalWriter
 * 前缀：cell：cell。。。。
 * 提供三种flush方式，对应操作系统刷新页到磁盘的三种方式
 * @author kq
 * @date 2019/6/10
 */
public interface IWalWriter {
    void write(Cell cell);
}
