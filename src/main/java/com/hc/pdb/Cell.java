package com.hc.pdb;

import com.hc.pdb.exception.NoEnoughByteException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBSerializeException;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author han.congcong
 */
public class Cell implements ISerializable, Comparable<Cell>{
    public static final long NO_TTL = -1;
    public static final byte[] DELETE_VALUE = new byte[1];

    private static final int NORMAL_BYTE = 1;
    private static final int DELETE_BYTE = 0;


    private byte[] key;
    private byte[] value;
    private long ttl;
    private long timeStamp;
    private boolean delete;

    public Cell(){}

    public Cell(byte[] key, byte[] value, long ttl, boolean delete) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
        this.delete = delete;
        this.timeStamp = System.currentTimeMillis();
    }
    public static Cell toCell(ByteBuffer currentBlock) {
        Cell cell = new Cell();
        cell.deSerialize(currentBlock);
        return cell;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) {
        if (byteBuffer.position() == byteBuffer.limit()) {
            throw new NoEnoughByteException();
        }
        byte type = byteBuffer.get();
        if(type == NORMAL_BYTE){
            createNormalCell(byteBuffer);
        }else {
            createDeleteCell(byteBuffer);
        }
    }

    private void createDeleteCell(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        int keyL = Bytes.toInt(bytes);
        byte[] key = new byte[keyL];
        byteBuffer.get(key);
        this.delete = true;
        this.key = key;
        this.ttl = NO_TTL;
    }

    private void createNormalCell(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[4];
        byteBuffer.get(bytes);
        int allL = Bytes.toInt(bytes);
        if (byteBuffer.position() + allL > byteBuffer.limit()) {
            throw new NoEnoughByteException("need byte:" + allL + " remain byte:"
                    + (byteBuffer.limit() - byteBuffer.position()));
        }
        byteBuffer.get(bytes);
        int keyL = Bytes.toInt(bytes);
        this.key = new byte[keyL];
        byteBuffer.get(key);
        this.value = new byte[allL - keyL - Long.BYTES * 2 - Integer.BYTES];
        byteBuffer.get(value);
        byte[] longBytes = new byte[8];
        byteBuffer.get(longBytes);
        this.ttl = Bytes.toLong(longBytes);
        byteBuffer.get(longBytes);
        this.timeStamp = Bytes.toLong(longBytes);
        this.delete = false;
    }


    @Override
    public byte[] serialize() throws PDBSerializeException {
        if(delete){
            return serializeDeleteCell();
        }
        return serializeNormalCell();
    }

    public byte[] serializeDeleteCell() throws PDBSerializeException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(DELETE_BYTE);
        try {
            outputStream.write(Bytes.toBytes(key.length));
            outputStream.write(key);
        }catch (IOException e){
            throw new PDBSerializeException(e);
        }
        return outputStream.toByteArray();
    }

    /**
     * key length + key timeToDrop + value
     */

    public byte[] serializeNormalCell() throws PDBSerializeException {
        //todo:抛弃output stream 的写法，写到文件里要增加version字段。
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(NORMAL_BYTE);
        try {
            outputStream.write(Bytes.toBytes(key.length + value.length + Long.BYTES * 2 + Integer.BYTES));
            outputStream.write(Bytes.toBytes(key.length));
            outputStream.write(key);
            outputStream.write(value);
            outputStream.write(Bytes.toBytes(ttl));
            outputStream.write(Bytes.toBytes(timeStamp));
        }catch (Exception e){
            throw new PDBSerializeException(e);
        }
        return outputStream.toByteArray();
    }


    @Override
    public int compareTo(Cell o) {
        int keyCompareResult = Bytes.compare(this.key, o.getKey());
        if(keyCompareResult == 0){
            return this.timeStamp - o.timeStamp > 0 ? 0 : 1;
        }
        return keyCompareResult;
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

    public boolean getDelete(){
        return delete;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
