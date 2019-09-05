package com.hc.pdb.hcc;

import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.TreeMap;

/**
 * Created by congcong.han on 2019/6/21.
 */
public class HCCFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCFile.class);
    /**
     * 文件路径
     */
    private String filePath;

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
    public HCCFile(String path, MetaReader metaReader) throws PDBIOException {
        this.filePath = path;
        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(path, "r");
            metaInfo = metaReader.read(file);

            LOGGER.info("meta info {}",metaInfo);
            preLoad(file);
        }catch (IOException e){
            throw new PDBIOException(e);
        }finally {
            if(file != null){
                try {
                    file.close();
                } catch (IOException e) {
                    throw new PDBIOException(e);
                }
            }
        }
    }

    private void preLoad(RandomAccessFile file) throws PDBIOException, IOException {
        //todo:check prefix
        checkFileStatus(file);
        //读取bloom过滤器
        loadBloom(file);
        //读取索引
        loadIndex(file);
    }

    private void checkFileStatus(RandomAccessFile file) throws PDBIOException, IOException {
        byte[] prefix = new byte[3];
        file.seek(0);
        file.readFully(prefix);
        if(Bytes.compare(prefix, FileConstants.HCC_WRITE_PREFIX) != 0){
            throw new PDBIOException("hcc file prefix not match!");
        }
    }

    private void loadBloom(RandomAccessFile file) throws PDBIOException {
        //todo: 和meta的代码一致，合并成一个
        try {
            byte[] metaLengthBytes = new byte[4];
            file.seek(file.length() - 4);
            file.readFully(metaLengthBytes);
            int metaL = Bytes.toInt(metaLengthBytes);

            int bloomStartIndex = metaInfo.getBloomStartIndex();
            int metaIndex = (int) file.length() - metaL - 4;
            LOGGER.info("begin to read bloom start {} end {}", bloomStartIndex, metaIndex);
            ByteBuffer bloomBytes = ByteBuffer.allocate(metaIndex - bloomStartIndex);
            file.getChannel().read(bloomBytes, bloomStartIndex);
            this.byteBloomFilter = new ByteBloomFilter(1, bloomBytes);
        }catch (IOException e){
            throw new PDBIOException(e);
        }
    }

    private void loadIndex(RandomAccessFile file) throws PDBIOException {
        int startIndex = metaInfo.getIndexStartIndex();
        int endIndex = metaInfo.getBloomStartIndex();
        LOGGER.info("begin to load index start {} end {}",startIndex, endIndex);
        ByteBuffer indexBuffer = ByteBuffer.allocate((endIndex - startIndex));
        indexBuffer.mark();
        try {
            file.getChannel().read(indexBuffer, startIndex);
        }catch (IOException e){
            throw new PDBIOException(e);
        }
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

    public HCCReader createReader() throws PDBIOException {
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
