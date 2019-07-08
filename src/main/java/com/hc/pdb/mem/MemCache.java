package com.hc.pdb.mem;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.CellUtil;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalWriter;
import org.checkerframework.common.util.report.qual.ReportOverride;

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

    private ConcurrentSkipListMap<byte[], Cell> memValue = new ConcurrentSkipListMap<>(new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return Bytes.compare(o1, o2);
        }
    });

    private AtomicLong size = new AtomicLong();
    private Configuration configuration;

    public MemCache(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "Configuration can not be null!");
        this.configuration = configuration;
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
        return memValue.subMap(startKey,true,endKey,false).entrySet().iterator();
    }

    public byte[] getStart(){
        return memValue.firstKey();
    }

    public byte[] getEnd(){
        return memValue.lastKey();
    }
}
