package com.hc.pdb.mem;


import com.hc.pdb.Cell;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.CellUtil;
import com.hc.pdb.wal.IWalReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;


/**
 * MemCache
 *
 * @author han.congcong
 * @date 2019/6/12
 */

public class MemCache{

    private String id = UUID.randomUUID().toString();

    private ConcurrentSkipListMap<byte[], Cell> memValue = new ConcurrentSkipListMap<>(Bytes::compare);

    private AtomicLong size = new AtomicLong();

    public MemCache(){}

    public MemCache(IWalReader reader) throws IOException {
        Iterator<Cell> iterator = reader.read();
        while(iterator.hasNext()){
            Cell cell = iterator.next();
            this.put(cell);
        }
    }

    public void put(Cell cell) {
        this.memValue.put(cell.getKey(), cell);
        size.addAndGet(CellUtil.calculateCellSize(cell));
    }

    public Collection<Cell> getAllCells() {
        return memValue.values();
    }

    public long size() {
        return size.get();
    }

    public Iterator<Map.Entry<byte[],Cell>> iterator(byte[] startKey, byte[] endKey){
        if(startKey == null && endKey == null){
            return memValue.entrySet().iterator();
        }
        if(startKey == null){
            startKey = memValue.firstKey();
        }
        boolean lastInclude = false;
        if(endKey == null ){
            lastInclude = true;
            endKey = memValue.lastKey();
        //get 请求
        }else if(Bytes.compare(startKey,endKey) == 0){
            lastInclude = true;
        }

        return memValue.subMap(startKey,true,endKey,lastInclude).entrySet().iterator();

    }

    public byte[] getStart(){
        if(!memValue.isEmpty()) {
            return memValue.firstKey();
        }else{
            return null;
        }
    }

    public byte[] getEnd(){
        if(!memValue.isEmpty()) {
            return memValue.lastKey();
        }else{
            return null;
        }
    }

    public String getId(){
        return id;
    }

    public boolean isEmpty(){
        return memValue.isEmpty();
    }

    @Override
    public boolean equals(Object o){
        if(o instanceof MemCache){
            return this.id.equals(((MemCache) o).getId());
        }
        return false;
    }
}
