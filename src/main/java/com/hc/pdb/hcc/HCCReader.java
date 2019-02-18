package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.TreeMap;

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
     * 索引
     */
    private TreeMap<byte[],Integer> key2index = new TreeMap<>(Bytes::compare);

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

    private void loadIndex() throws IOException {
        int startIndex = metaInfo.getIndexStartIndex();
        int endIndex = metaInfo.getBloomStartIndex();
        ByteBuffer indexBuffer = ByteBuffer.allocate((endIndex - startIndex));
        file.getChannel().read(indexBuffer,startIndex);
        while(indexBuffer.position() < indexBuffer.limit()){
            int keyL = indexBuffer.getInt();
            byte[] key = new byte[keyL];
            indexBuffer.get(key);
            int index = indexBuffer.getInt();
            this.key2index.put(key,index);
        }
    }

    private void loadBloom() throws IOException {
        int bloomStartIndex = metaInfo.getBloomStartIndex();
        int metaIndex = (int) file.length() - MetaInfo.META_SIZE - bloomStartIndex;
        ByteBuffer bloomBytes = ByteBuffer.allocate(bloomStartIndex - metaIndex);
        file.getChannel().read(bloomBytes,bloomStartIndex);
        this.byteBloomFilter = new ByteBloomFilter(1,bloomBytes);
    }

    @Override
    public Result exist(byte[] key) {
        if(byteBloomFilter.contains(key)){
            return Result.dontKnow;
        }
        return Result.not_exist;
    }

    @Override
    public Cell next(byte[] key) throws IOException {
        //1 找到key的index
        int start = this.key2index.lowerEntry(key).getValue();
        int end = this.key2index.higherEntry(key).getValue();
        //2 读取到block
        List<Cell> cells = readCell(start,end);

        //3 遍历cell，查询key的cell
        return null;
    }

    private List<Cell> readCell(int start, int end) throws IOException {
        ByteBuffer blockBuffer = ByteBuffer.allocate(end - start);
        file.getChannel().read(blockBuffer,start);
        //todo:
        return null;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
