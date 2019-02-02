package com.hc.pdb.hcc;

import java.io.FileNotFoundException;
import java.io.IOException;

public interface HCCReader {
    /**
     * load bloom filter，index等信息
     */
    void open() throws IOException;

    /**
     * 一次检索
     * @param start
     * @param ends
     */
    void scan(byte[] start, byte[] ends);

    /**
     *  关闭
     */
    void close();
}
