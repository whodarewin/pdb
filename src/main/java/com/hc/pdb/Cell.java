package com.hc.pdb;


import com.hc.pdb.exception.NoEnoughByteException;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Cell {

    private byte[] key;
    private byte[] value;
    private long ttl;


    public Cell(byte[] key, byte[] value, long ttl) {
        this.key = key;
        this.value = value;
        this.ttl = System.currentTimeMillis() + ttl;

    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTimeToDrop() {
        return ttl;
    }
    //key length + key timeToDrop + value
    public byte[] toByte() throws IOException {
        //todo:抛弃output stream 的写法，写到文件里要增加version字段。

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(key.length + value.length + 4);
        outputStream.write(key.length);
        outputStream.write(key);
        outputStream.write(value);
        outputStream.write(Bytes.toBytes(ttl));
        return outputStream.toByteArray();
    }

    public static Cell toCell(ByteBuffer byteBuffer){
        if(byteBuffer.position() == byteBuffer.limit()){
            return null;
        }
        int allL = byteBuffer.getInt();
        if(byteBuffer.position() + allL > byteBuffer.limit()){
            throw new NoEnoughByteException("need byte:" + allL+" remain byte:"
                    + (byteBuffer.limit() - byteBuffer.position()));
        }
        int keyL = byteBuffer.getInt();
        byte[] key = new byte[keyL];
        byteBuffer.get(key);
        byte[] value = new byte[allL - keyL - Long.BYTES];
        byteBuffer.get(value);
        long ttl = byteBuffer.getLong();
        return new Cell(key,value,ttl);
    }

    public static byte[] readKey(ByteBuffer byteBuffer){
        if(byteBuffer.position() == byteBuffer.limit()){
            return null;
        }

        int allL = byteBuffer.getInt();
        if(byteBuffer.position() + allL > byteBuffer.limit()){
            throw new NoEnoughByteException("need byte:" + allL+" remain byte:"
                    + (byteBuffer.limit() - byteBuffer.position()));
        }
        int keyL = byteBuffer.getInt();
        byte[] key = new byte[keyL];
        byteBuffer.get(key);
        return key;
    }


}
