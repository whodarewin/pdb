package com.hc.pdb.mem;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.CellUtil;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalReader;
import com.hc.pdb.wal.IWalWriter;
import org.checkerframework.common.util.report.qual.ReportOverride;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.sun.corba.se.impl.util.RepositoryId.cache;


/**
 * MemCache
 *
 * @author han.congcong
 * @date 2019/6/12
 */

public class MemCache{

    private ConcurrentSkipListMap<byte[], Cell> memValue = new ConcurrentSkipListMap<>((o1, o2) -> Bytes.compare(o1, o2));

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
        if(endKey == null || Bytes.compare(startKey,endKey) == 0 ){
            lastInclude = true;
            endKey = memValue.lastKey();
        }

        return memValue.subMap(startKey,true,endKey,lastInclude).entrySet().iterator();

    }

    public byte[] getStart(){
        return memValue.firstKey();
    }

    public byte[] getEnd(){
        return memValue.lastKey();
    }
}
