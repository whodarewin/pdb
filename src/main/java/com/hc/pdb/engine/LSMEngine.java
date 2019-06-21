package com.hc.pdb.engine;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.flusher.IFlusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalWriter;

import java.io.File;
import java.io.IOException;

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
    private IWalWriter walWriter;

    public LSMEngine(Configuration configuration) throws IOException {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        memCache = new MemCache(configuration);
        hccWriter = new HCCWriter(configuration);
        flusher = new Flusher(configuration, hccWriter);
        this.walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));

    }

    @Override
    public void put(byte[] key, byte[] value, long ttl) throws IOException {
        Cell cell = new Cell(key, value, ttl,false);
        walWriter.write(cell);
        this.memCache.put(cell);
        //flush
        flushIfOK();
    }
    //todo:同步问题
    @Override
    public void clean() {
        String path = configuration.get(PDBConstants.DB_PATH_KEY);
        File dir = new File(path);
        dir.delete();
    }

    private void flushIfOK() throws IOException {
        if (memCache.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (memCache.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
                    MemCache tmpCache = memCache;
                    IWalWriter tmpWalWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
                    memCache = new MemCache(configuration);
                    flusher.flush(new IFlusher.FlushEntry(tmpCache,tmpWalWriter));
                }
            }
        }
    }

    @Override
    public byte[] get(byte[] key) {
        return null;
    }

    @Override
    public void delete(byte[] key) throws IOException {
        Cell cell = new Cell(key, null, Cell.NO_TTL, false);
        walWriter.write(cell);
        this.memCache.put(cell);
        //flush
        flushIfOK();
    }

    @Override
    public IScanner scan(byte[] start, byte[] end) {

        return null;
    }

}
