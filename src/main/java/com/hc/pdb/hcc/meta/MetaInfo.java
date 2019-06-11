package com.hc.pdb.hcc.meta;

import com.hc.pdb.ISerializable;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * meta 信息
 */
public class MetaInfo implements ISerializable {
    /**
     * 创建时间
     */
    private long createTime;
    /**
     * 整个hcc的startKey
     */
    private byte[] startKey;
    /**
     * 整个hcc的endKey
     */
    private byte[] endKey;
    /**
     * 索引的startKey
     */
    private int indexStartIndex;
    /**
     * 不聋过滤器的startKey
     */
    private int bloomStartIndex;

    public MetaInfo(){}

    /**
     * 创建metainfo
     *
     * @param createTime
     * @param startKey
     * @param endKey
     * @param indexStartIndex
     * @param bloomStartIndex
     */
    public MetaInfo(long createTime, byte[] startKey, byte[] endKey, int indexStartIndex, int bloomStartIndex) {
        this.createTime = createTime;
        this.startKey = startKey;
        this.endKey = endKey;
        this.indexStartIndex = indexStartIndex;
        this.bloomStartIndex = bloomStartIndex;
    }



    public int getIndexStartIndex() {
        return indexStartIndex;
    }

    public void setIndexStartIndex(int indexStartIndex) {
        this.indexStartIndex = indexStartIndex;
    }

    public int getBloomStartIndex() {
        return bloomStartIndex;
    }

    public void setBloomStartIndex(int bloomStartIndex) {
        this.bloomStartIndex = bloomStartIndex;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public void setStartKey(byte[] startKey) {
        this.startKey = startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public void setEndKey(byte[] endKey) {
        this.endKey = endKey;
    }

    /**
     * 序列化
     * start key length | start key | end key length | end key | index start index | bloom start index
     * @return
     */
    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Bytes.toBytes(createTime));
        outputStream.write(Bytes.toBytes(startKey.length));
        outputStream.write(startKey);
        outputStream.write(Bytes.toBytes(endKey.length));
        outputStream.write(endKey);
        outputStream.write(Bytes.toBytes(indexStartIndex));
        outputStream.write(Bytes.toBytes(bloomStartIndex));
        return outputStream.toByteArray();
    }

    /**
     *
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    public void deSerialize(ByteBuffer buffer) {

        byte[] createTimeBytes = new byte[8];
        buffer.get(createTimeBytes);
        long createTime = Bytes.toLong(createTimeBytes);
        byte[] sklBytes = new byte[4];
        buffer.get(sklBytes);
        int skl = Bytes.toInt(sklBytes);
        byte[] startK = new byte[skl];
        buffer.get(startK);
        byte[] eklBytes = new byte[4];
        buffer.get(eklBytes);
        int ekl = Bytes.toInt(eklBytes);
        byte[] endK = new byte[ekl];
        buffer.get(endK);
        byte[] indexStartBytes = new byte[4];
        byte[] bloomStartBytes = new byte[4];
        buffer.get(indexStartBytes);
        buffer.get(bloomStartBytes);
        int indexStartIndex = Bytes.toInt(indexStartBytes);
        int bloomStartIndex = Bytes.toInt(bloomStartBytes);
        this.createTime = createTime;
        this.startKey = startK;
        this.endKey = endK;
        this.indexStartIndex = indexStartIndex;
        this.bloomStartIndex = bloomStartIndex;
    }

    public static MetaInfo toMetaInfo(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.mark();
        buffer.put(bytes);
        buffer.reset();
        MetaInfo info = new MetaInfo();
        info.deSerialize(buffer);
        return info;
    }

    @Override
    public String toString(){
        return "index start at " + indexStartIndex
                + "\nbloom start at " + bloomStartIndex;
    }

    public int size(){
        int startKeyLength = startKey == null ? 0 : startKey.length;
        int endKeyLength = endKey == null ? 0 : endKey.length;
        return startKeyLength  + endKeyLength + 8;
    }
}
