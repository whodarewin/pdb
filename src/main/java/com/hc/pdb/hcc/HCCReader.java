package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
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
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;

/**
 * HCCReader
 * 读取一个hcc文件
 * todo:不要魔法值
 * @author han.congcong
 * @date 2019/6/5
 */

public class HCCReader implements IHCCReader {
    private static final Logger LOGGER  = LoggerFactory.getLogger(HCCReader.class);

    private String filePath;

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
     * file channel
     */
    private FileChannel channel;
    /**
     * 索引 block的开始key和blcok的开始index
     */
    private TreeMap<byte[], Integer> key2index;
    /**
     * block的start
     */
    private int blockStartIndex;
    /**
     * block的end
     */
    private int blockEndIndex;
    /**
     *
     */
    private byte[] endKey;
    /**
     * 当前读取的block
     */
    private ByteBuffer currentBlock;

    /**
     * 加载预加载内容
     *
     * @param path hcc 的地址
     * @param key2index block的索引
     * @param byteBloomFilter 布隆过滤器
     * @param metaInfo hcc 的 META值
     */
    public HCCReader(String path, TreeMap<byte[], Integer> key2index,
                     ByteBloomFilter byteBloomFilter,MetaInfo metaInfo) throws PDBIOException {
        this.filePath = path;
        try {
            file = new RandomAccessFile(path, "r");
        }catch (Exception e){
            throw new PDBIOException(e);
        }
        this.channel = file.getChannel();
        this.key2index = key2index;
        this.byteBloomFilter = byteBloomFilter;
        this.metaInfo = metaInfo;
        seekToFirst();
    }

    private void seekToFirst() throws PDBIOException {
        //找到block的start index，为啥不直接用startKey找：TODO:
        blockStartIndex = this.key2index.firstEntry().getValue();
        //找到第二个entry
        Map.Entry<byte[], Integer> entry = this.key2index.higherEntry(this.metaInfo.getStartKey());
        //如果能找到，则其start即为这个block的end
        if (entry != null) {
            endKey = entry.getKey();
            blockEndIndex = entry.getValue();
        //如果找不到
        } else {
            endKey = null;
            //todo:为何这个要减1，而上面的entry.getValue 不需要？
            blockEndIndex = metaInfo.getIndexStartIndex() - 1;
        }
        LOGGER.info("seek to first {} {}", blockStartIndex, blockEndIndex);
        readBlock(blockStartIndex, blockEndIndex);
    }

    @Override
    public Result exist(byte[] key) {
        if (byteBloomFilter.contains(key)) {
            return Result.dontKnow;
        }
        return Result.not_exist;
    }

    @Override
    public void seek(byte[] key) throws PDBIOException {
        if(key == null || Bytes.compare(key,metaInfo.getStartKey()) <= 0){
            seekToFirst();
            return;
        }
        if (Bytes.compare(key, metaInfo.getEndKey()) > 0) {
            throw new KeyOutofRangeException("end key out of range,should not be here");
        }

        //1 找到key所定义的index
        Map.Entry<byte[],Integer> startEntry = this.key2index.lowerEntry(key);
        Map.Entry<byte[],Integer> endEntry = this.key2index.higherEntry(key);

        if(startEntry == null){
            //不可能的，永远不会为null
            blockStartIndex = FileConstants.HCC_WRITE_PREFIX.length;
        }else{
            blockStartIndex = startEntry.getValue();
        }
        if(endEntry == null){
            // 也是不可能的，永远不会为null
            blockEndIndex = metaInfo.getIndexStartIndex() - 1;
        }else{
            blockEndIndex = endEntry.getValue();
            endKey = endEntry.getKey();
        }
        LOGGER.info("seek to block {} {}", blockStartIndex, blockEndIndex);
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

    private void readBlock(int blockStartIndex, int blockEndIndex) {
        int cap = blockEndIndex - blockStartIndex;
        this.currentBlock = ByteBuffer.allocateDirect(cap);
        this.currentBlock.mark();
        try {
            channel.read(this.currentBlock, blockStartIndex);
        }catch (IOException e){

        }
        this.currentBlock.reset();
    }


    @Override
    public Cell next() {
        if(this.currentBlock.position() == this.currentBlock.limit()){
            blockStartIndex = blockEndIndex ;
            Map.Entry<byte[],Integer> entry = key2index.higherEntry(endKey);
            if(entry == null){
                LOGGER.info("hcc read over {}",filePath);
                return null;
            }
            blockEndIndex = entry.getValue();
            endKey =  entry.getKey();
            LOGGER.debug("current block read over,read next begin {} end {}", blockStartIndex, blockEndIndex);
            readBlock(blockStartIndex,blockEndIndex);
        }
        return Cell.toCell(currentBlock);
    }
    @Override
    public void close() throws IOException {
        file.close();
    }
}
