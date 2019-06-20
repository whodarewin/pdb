package com.hc.pdb;

import com.hc.pdb.exception.NoEnoughByteException;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Cell {
    public static final long NO_TTL = -1;

    private static final int NORMAL_BYTE = 1;
    private static final int DELETE_BYTE = 0;

    private byte[] key;
    private byte[] value;
    private long ttl;
    private boolean delete;

    public Cell(byte[] key, byte[] value, long ttl,boolean delete) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.delete = delete;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTtl() {
        return ttl;
    }

    //key length + key timeToDrop + value
    public byte[] toBytes() throws IOException {
        if(delete){
            return toDeleteBytes();
        }
        return toNormalBytes();
    }

    private byte[] toDeleteBytes() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(DELETE_BYTE);
        outputStream.write(Bytes.toBytes(key.length));
        outputStream.write(key);
        return outputStream.toByteArray();
    }

    private byte[] toNormalBytes() throws IOException {
        //todo:抛弃output stream 的写法，写到文件里要增加version字段。
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(NORMAL_BYTE);
        outputStream.write(Bytes.toBytes(key.length + value.length + Long.BYTES));
        outputStream.write(Bytes.toBytes(key.length));
        outputStream.write(key);
        outputStream.write(value);
        outputStream.write(Bytes.toBytes(ttl));
        return outputStream.toByteArray();
    }

    public static Cell toCell(ByteBuffer byteBuffer) {
        if (byteBuffer.position() == byteBuffer.limit()) {
            return null;
        }
        byte type = byteBuffer.get();
        if(type == NORMAL_BYTE){
            return createNormalCell(byteBuffer);
        }
        return createDeleteCell(byteBuffer);
    }

    private static Cell createDeleteCell(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        int keyL = Bytes.toInt(bytes);
        byte[] key = new byte[keyL];
        byteBuffer.get(key);
        return new Cell(key,null,NO_TTL,true);
    }

    private static Cell createNormalCell(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        int allL = Bytes.toInt(bytes);
        if (byteBuffer.position() + allL > byteBuffer.limit()) {
            throw new NoEnoughByteException("need byte:" + allL + " remain byte:"
                    + (byteBuffer.limit() - byteBuffer.position()));
        }
        byteBuffer.get(bytes);
        int keyL = Bytes.toInt(bytes);
        byte[] key = new byte[keyL];
        byteBuffer.get(key);
        byte[] value = new byte[allL - keyL - Long.BYTES];
        byteBuffer.get(value);
        byte[] longBytes = new byte[8];
        byteBuffer.get(longBytes);
        long ttl = Bytes.toLong(longBytes);
        return new Cell(key, value, ttl, false);
    }
}
