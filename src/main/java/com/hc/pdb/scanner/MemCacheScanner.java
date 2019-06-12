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
    private MemCache memCache;
    private Iterator<Map.Entry<byte[],Cell>> iterator;
    private Cell current;

    public MemCacheScanner(MemCache memCache) {
        this.memCache = memCache;
        this.iterator = memCache.iterator();
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

