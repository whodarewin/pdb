package com.hc.pdb.hcc;

import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;

import java.io.IOException;
import java.io.RandomAccessFile;

public class HCCReader implements IHCCReader {
    private MetaInfo metaInfo;
    private ByteBloomFilter byteBloomFilter;
    private RandomAccessFile file;
    private ByteBloomFilter filter;

    /**
     * 加载预加载内容
     * @param path hcc 的地址
     */
    public HCCReader(String path,MetaReader metaReader) throws IOException {
        file  = new RandomAccessFile(path,"r");
        metaInfo = metaReader.read(file);
        preLoad();

    }

    private void preLoad() throws IOException {
        //读取bloom过滤器
        loadBloom();
        //读取索引
        loadIndex();
    }

    private void loadIndex() {

    }

    private void loadBloom() throws IOException {
        int bloomStartIndex = metaInfo.getBloomStartIndex();
        int metaIndex = (int) file.length() - 64;
        byte[] bloomBytes = new byte[bloomStartIndex - metaIndex];
        file.read(bloomBytes,bloomStartIndex,bloomBytes.length);
    }

    @Override
    public Result exist(byte[] key) {
        return null;
    }

    @Override
    public void next(byte[] key) {

    }

    @Override
    public void close() {

    }
}
