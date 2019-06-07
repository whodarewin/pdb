package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.exception.KeyOutofRangeException;
import com.hc.pdb.exception.SeekOutofRangeException;
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
import java.util.Map;
import java.util.TreeMap;


/**
 * HCCReader
 * 读取一个hcc文件
 * @author han.congcong
 * @date 2019/6/5
 */

public class HCCReader implements IHCCReader {
    private static final Logger LOGGER  = LoggerFactory.getLogger(HCCReader.class);

    private MetaInfo metaInfo;
    /**
     * bloom filter
     */
    private ByteBloomFilter byteBloomFilter;
    /**
     * file
     */
    private RandomAccessFile file;
    /**
     * 索引 block的开始key和blcok的开始index
     */
    private TreeMap<byte[], Integer> key2index = new TreeMap<>(Bytes::compare);

    private int blockStartIndex;
    private int blockEndIndex;
    private byte[] endKey;
    private ByteBuffer currentBlock;

    /**
     * 加载预加载内容
     *
     * @param path hcc 的地址
     */
    public HCCReader(String path, MetaReader metaReader) throws IOException {
        file = new RandomAccessFile(path, "r");
        metaInfo = metaReader.read(file);
        LOGGER.info("meta info readed {}",metaInfo);
        preLoad();
    }

    private void preLoad() throws IOException {
        //读取bloom过滤器
        loadBloom();
        //读取索引
        loadIndex();
        seekToFirst();
    }

    private void seekToFirst() throws IOException {
        //1 找到key所定义的index
        blockStartIndex = this.key2index.firstEntry().getValue();
        Map.Entry<byte[],Integer> entry = this.key2index.higherEntry(this.metaInfo.getStartKey());
        if(entry != null) {
            endKey = entry.getKey();
            blockEndIndex = entry.getValue();
        }else{
            endKey = null;
            blockEndIndex = metaInfo.getIndexStartIndex() - 1;
        }
        readBlock(blockStartIndex,blockEndIndex);
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

    @Override
    public Result exist(byte[] key) {
        if (byteBloomFilter.contains(key)) {
            return Result.dontKnow;
        }
        return Result.not_exist;
    }

    @Override
    public void seek(byte[] key) throws IOException {
        if (Bytes.compare(key, metaInfo.getEndKey()) > 0) {
            throw new KeyOutofRangeException();
        }
        //1 找到key所定义的index
        Map.Entry<byte[],Integer> startEntry = this.key2index.lowerEntry(key);
        Map.Entry<byte[],Integer> endEntry = this.key2index.higherEntry(key);

        if(startEntry == null){
            blockStartIndex = FileConstants.HCC_WRITE_PREFIX.length;
        }else{
            blockStartIndex = startEntry.getValue();
        }
        if(endEntry == null){
            blockEndIndex = metaInfo.getIndexStartIndex() - 1;
        }else{
            blockEndIndex = endEntry.getValue();
        }

        readBlock(blockStartIndex,blockEndIndex);
        seekBlock(key);
    }

    private void seekBlock(byte[] seekkey) {
        Cell cell = null;
        int position = currentBlock.position();
        while ((cell = Cell.toCell(currentBlock)) != null) {
            if(Bytes.compare(cell.getKey(), seekkey) == 0){
                currentBlock.position(position);
                return;
            }
            if (Bytes.compare(cell.getKey(), seekkey) > 0) {
                return;
            }
            position = currentBlock.position();
        }
        throw new SeekOutofRangeException();
    }

    private void readBlock(int blockStartIndex, int blockEndIndex) throws IOException {
        this.currentBlock = ByteBuffer.allocate(blockEndIndex - blockStartIndex);
        this.currentBlock.mark();
        file.getChannel().read(this.currentBlock, blockStartIndex);
        this.currentBlock.reset();
    }


    @Override
    public Cell next() throws IOException {
        if(this.currentBlock.position() == this.currentBlock.limit()){
            blockStartIndex = blockEndIndex + 1;
            Map.Entry<byte[],Integer> entry = key2index.lowerEntry(endKey);
            blockEndIndex = entry.getValue();
            endKey =  entry.getKey();
            readBlock(blockStartIndex,blockEndIndex);
        }
        return Cell.toCell(currentBlock);
    }
    @Override
    public void close() throws IOException {
        file.close();
    }
}
