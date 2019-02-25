package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
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
     * 索引 block的开始key和blcok的开始index
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
        //1 找到key所定义的index
        Integer start = this.key2index.lowerEntry(key).getValue();
        Integer end = this.key2index.higherEntry(key).getValue();
        //2 读取到block
        Cell ret = readCell(start,end, key);


        return ret;
    }

    private Cell readCell(int start, int end, byte[] theKey) throws IOException {
        ByteBuffer blockBuffer = ByteBuffer.allocate(end - start);
        file.getChannel().read(blockBuffer,start);
        byte[] key = null;
        int position = blockBuffer.position();
        while((key = Cell.readKey(blockBuffer)) != null){
            if(Bytes.compare(key,theKey) > 0){
                blockBuffer.position(position);
                return Cell.toCell(blockBuffer);
            }
            position = blockBuffer.position();
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
