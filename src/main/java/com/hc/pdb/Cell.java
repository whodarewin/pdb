package com.hc.pdb;

import com.hc.pdb.exception.NoEnoughByteException;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Cell
 * todo:如何做delete
 * @author han.congcong
 * @date 2019/6/11
 */

public class Cell implements ISerializable{

    private byte[] key;
    private byte[] value;
    private long ttl;

    public Cell(){}

    public Cell(byte[] key, byte[] value, long ttl) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;

    }

    public static Cell toCell(ByteBuffer currentBlock) {
        Cell cell = new Cell();
        cell.deSerialize(currentBlock);
        return cell;
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

    /**
     * key length + key timeToDrop + value
     */
    @Override
    public byte[] serialize() throws IOException{
        //todo:抛弃output stream 的写法，写到文件里要增加version字段。
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Bytes.toBytes(key.length + value.length + Long.BYTES));
        outputStream.write(Bytes.toBytes(key.length));
        outputStream.write(key);
        outputStream.write(value);
        outputStream.write(Bytes.toBytes(ttl));
        return outputStream.toByteArray();
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) {
        if (byteBuffer.position() == byteBuffer.limit()) {
            throw new NoEnoughByteException();
        }
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
        this.key = key;
        this.value = value;
        this.ttl = ttl;
    }
}
