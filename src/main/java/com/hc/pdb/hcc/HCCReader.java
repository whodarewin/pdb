package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.exception.KeyOutofRangeException;
import com.hc.pdb.exception.SeekOutofRangeException;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;

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
        Map.Entry<byte[],Integer> entry = this.key2index.lowerEntry(this.metaInfo.getStartKey());
        endKey = entry.getKey();
        blockEndIndex = entry.getValue();
        readBlock(blockStartIndex,blockEndIndex);
    }

    private void loadIndex() throws IOException {
        int startIndex = metaInfo.getIndexStartIndex();
        int endIndex = metaInfo.getBloomStartIndex();
        ByteBuffer indexBuffer = ByteBuffer.allocate((endIndex - startIndex));
        indexBuffer.mark();
        file.getChannel().read(indexBuffer, startIndex);
        indexBuffer.reset();
        while (indexBuffer.position() < indexBuffer.limit()) {
            int keyL = indexBuffer.getInt();
            byte[] key = new byte[keyL];
            indexBuffer.get(key);
            int index = indexBuffer.getInt();
            this.key2index.put(key, index);
        }
    }

    private void loadBloom() throws IOException {
        int bloomStartIndex = metaInfo.getBloomStartIndex();
        int metaIndex = (int) file.length() - MetaInfo.META_SIZE - bloomStartIndex;
        ByteBuffer bloomBytes = ByteBuffer.allocate(bloomStartIndex - metaIndex);
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
        if (Bytes.compare(key, metaInfo.getEndKey()) < 0) {
            throw new KeyOutofRangeException();
        }
        //1 找到key所定义的index
        blockStartIndex = this.key2index.lowerEntry(key).getValue();
        blockEndIndex = this.key2index.higherEntry(key).getValue();
        readBlock(blockStartIndex,blockEndIndex);
        seekBlock(key);
    }

    private void seekBlock(byte[] seekkey) {
        byte[] key = null;
        int position = currentBlock.position();
        while ((key = Cell.readKey(currentBlock)) != null) {
            if (Bytes.compare(key, seekkey) > 0) {
                currentBlock.position(position);
                return;
            }
            position = currentBlock.position();
        }
        throw new SeekOutofRangeException();
    }

    private void readBlock(int blockStartIndex, int blockEndIndex) throws IOException {
        this.currentBlock = ByteBuffer.allocate(blockEndIndex - blockStartIndex);
        file.getChannel().read(this.currentBlock, blockStartIndex);
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
