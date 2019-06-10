package com.hc.pdb.hcc.meta;

import com.hc.pdb.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * meta 信息
 */
public class MetaInfo {
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
     * @param bytes
     * @return
     * @throws IOException
     */
    public static MetaInfo deSerialize(byte[] bytes) throws IOException {
        ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);
        byte[] createTimeBytes = new byte[8];
        byteInputStream.read(createTimeBytes);
        long createTime = Bytes.toLong(createTimeBytes);
        byte[] sklBytes = new byte[4];
        byteInputStream.read(sklBytes);
        int skl = Bytes.toInt(sklBytes);
        byte[] startK = new byte[skl];
        byteInputStream.read(startK);
        byte[] eklBytes = new byte[4];
        byteInputStream.read(eklBytes);
        int ekl = Bytes.toInt(eklBytes);
        byte[] endK = new byte[ekl];
        byteInputStream.read(endK);
        byte[] indexStartBytes = new byte[4];
        byte[] bloomStartBytes = new byte[4];
        byteInputStream.read(indexStartBytes);
        byteInputStream.read(bloomStartBytes);
        int indexStartIndex = Bytes.toInt(indexStartBytes);
        int bloomStartIndex = Bytes.toInt(bloomStartBytes);

        return new MetaInfo(createTime, startK, endK, indexStartIndex, bloomStartIndex);
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
