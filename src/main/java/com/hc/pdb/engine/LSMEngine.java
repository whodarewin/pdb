package com.hc.pdb.engine;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.flusher.IFlusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;

/**
 * LSMEngine
 * LSM数的engine
 * todo:需要事物操作日志，供断电回滚使用
 * @author han.congcong
 * @date 2019/6/3
 */

public class LSMEngine implements IEngine {

    private Configuration configuration;
    private volatile MemCache memCache;
    private HCCWriter hccWriter;
    private IFlusher flusher;

    public LSMEngine(Configuration configuration) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        memCache = new MemCache(configuration);
        hccWriter = new HCCWriter(configuration);
        flusher = new Flusher(configuration, hccWriter);

    }

    @Override
    public void put(byte[] key, byte[] value, long ttl) {
        this.memCache.put(new Cell(key, value, ttl));
        //flush
        if (memCache.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (memCache.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
                    MemCache tmpCache = memCache;
                    memCache = new MemCache(configuration);
                    flusher.flush(tmpCache);
                }
            }
        }
    }

    public Cell get(byte[] key) {
        return null;
    }

    public void delete(byte[] key) {

    }

}
