package com.hc.pdb.engine;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.flusher.IFlusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;

public class Engine {

    private Configuration configuration;
    private volatile MemCache memCache;
    private HCCWriter hccWriter;
    private IFlusher flusher;

    public Engine(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        memCache = new MemCache(configuration);
        hccWriter = new HCCWriter(configuration);
        flusher = new Flusher(configuration,hccWriter);

    }

    public void put(byte[] key, byte[] value, long ttl){
        this.memCache.put(new Cell(key,value,ttl));
        //flush
        if(memCache.size() > configuration.getLong(Constants.MEM_CACHE_MAX_SIZE_KEY,
                Constants.DEFAULT_MEM_CACHE_MAX_SIZE)){
            synchronized (this) {
                if(memCache.size() > configuration.getLong(Constants.MEM_CACHE_MAX_SIZE_KEY,
                        Constants.DEFAULT_MEM_CACHE_MAX_SIZE)){
                    MemCache tmpCache = memCache;
                    memCache = new MemCache(configuration);
                    flusher.flush(tmpCache);
                }
            }
        }
    }

    public Cell get(byte[] key){
        return null;
    }

    public void delete(byte[] key){

    }
}
