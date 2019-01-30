package com.hc.pdb;


import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Cell {

    private byte[] key;
    private byte[] value;
    private long timeToDrop;


    public Cell(byte[] key, byte[] value, long ttl) {
        this.key = key;
        this.value = value;
        this.timeToDrop = System.currentTimeMillis() + ttl;

    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getTimeToDrop() {
        return timeToDrop;
    }
    //key length + key timeToDrop + value
    public byte[] toByte() throws IOException {
        //todo:抛弃output stream 的写法，写到文件里要增加version字段。
        int keyLength = key.length;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(keyLength);
        outputStream.write(key);
        outputStream.write(Bytes.toBytes(timeToDrop));
        outputStream.write(value);
        return outputStream.toByteArray();
    }
}
