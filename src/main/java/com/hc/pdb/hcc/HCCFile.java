package com.hc.pdb.hcc;

import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * Created by congcong.han on 2019/6/21.
 */
public class HCCFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCFile.class);

    private String filePath;

    private RandomAccessFile file;

    private MetaInfo metaInfo;
    /**
     * bloom filter
     */
    private ByteBloomFilter byteBloomFilter;
    /**
     * 索引 block的开始key和blcok的开始index
     */
    private TreeMap<byte[], Integer> key2index = new TreeMap<>(Bytes::compare);

    private byte[] start;

    /**
     * 加载预加载内容
     *
     * @param path hcc 的地址
     */
    public HCCFile(String path, MetaReader metaReader) throws IOException {
        this.filePath = path;
        file = new RandomAccessFile(path, "r");
        metaInfo = metaReader.read(file);
        LOGGER.info("meta info {}",metaInfo);
        preLoad();
    }

    private void preLoad() throws IOException {
        //todo:check prefix
        //读取bloom过滤器
        loadBloom();
        //读取索引
        loadIndex();
    }

    private void loadBloom() throws IOException {
        //todo: 和meta的代码一致，合并成一个
        byte[] metaLengthBytes = new byte[4];
        file.seek(file.length() - 4);
        file.readFully(metaLengthBytes);
        int metaL = Bytes.toInt(metaLengthBytes);

        int bloomStartIndex = metaInfo.getBloomStartIndex();
        int metaIndex = (int) file.length() - metaL - 4;
        LOGGER.info("begin to read bloom start {} end {}",bloomStartIndex, metaIndex);
        ByteBuffer bloomBytes = ByteBuffer.allocate(metaIndex - bloomStartIndex);
        file.getChannel().read(bloomBytes, bloomStartIndex);
        this.byteBloomFilter = new ByteBloomFilter(1, bloomBytes);
    }

    private void loadIndex() throws IOException {
        int startIndex = metaInfo.getIndexStartIndex();
        int endIndex = metaInfo.getBloomStartIndex();
        LOGGER.info("begin to load index start {} end {}",startIndex, endIndex);
        ByteBuffer indexBuffer = ByteBuffer.allocate((endIndex - startIndex));
        indexBuffer.mark();
        file.getChannel().read(indexBuffer, startIndex);
        indexBuffer.reset();
        while (indexBuffer.position() < indexBuffer.limit()) {
            byte[] bytes = new byte[4];
            indexBuffer.get(bytes);
            int keyL = Bytes.toInt(bytes);
            byte[] key = new byte[keyL];
            indexBuffer.get(key);
            indexBuffer.get(bytes);
            int index = Bytes.toInt(bytes);
            this.key2index.put(key, index);
        }
    }

    public HCCReader createReader() throws IOException {
        return new HCCReader(filePath,key2index,byteBloomFilter,metaInfo);
    }

    public String getFilePath(){
        return filePath;
    }

    public byte[] getStart() {
        return metaInfo.getStartKey();
    }

    public byte[] getEnd(){
        return metaInfo.getEndKey();
    }
}
