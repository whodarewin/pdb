package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.mem.MemCache;
import java.util.Iterator;
import java.util.Map;

/**
 * MemCacheScanner
 * memCache 的scanner
 * @author han.congcong
 * @date 2019/6/12
 */
public class MemCacheScanner implements IScanner{
    private Iterator<Map.Entry<byte[],Cell>> iterator;
    private Cell current;

    public MemCacheScanner(MemCache memCache, byte[] startKey, byte[] endKey) {
        this.iterator = memCache.iterator(startKey,endKey);
    }

    @Override
    public Cell next() {
        if(iterator.hasNext()){
            current = iterator.next().getValue();
        }else{
            return null;
        }
        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }
}

