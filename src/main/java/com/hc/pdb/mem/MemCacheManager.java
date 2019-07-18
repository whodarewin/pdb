package com.hc.pdb.mem;

import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.flusher.IFlusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.RangeUtil;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * mem cache的manager
 * @author congcong.han
 * @date 2019/6/22
 */
public class MemCacheManager {

    private IFlusher flusher;
    private IWalWriter walWriter;
    private StateManager stateManager;
    private Configuration configuration;
    private List<MemCache> flushingList = Collections.synchronizedList(new ArrayList<>());
    private MemCache current;
    private HCCWriter hccWriter;

    public MemCacheManager(Configuration configuration,StateManager manager, HCCWriter hccWriter) throws IOException {
        this.hccWriter = hccWriter;
        this.stateManager = manager;
        flusher = new Flusher(configuration, hccWriter, stateManager);
        this.walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
        current = new MemCache(configuration);
        this.configuration = configuration;
    }

    public Set<MemCache> searchMemCache(byte[] startKey, byte[] endKey){
        Set<MemCache> sets =  flushingList.stream().filter(cache ->
                RangeUtil.inOpenCloseInterval(cache.getStart(),cache.getEnd(),startKey,endKey))
                .collect(Collectors.toSet());
        if(RangeUtil.inOpenCloseInterval(current.getStart(),current.getEnd(),startKey,endKey)){
            sets.add(current);
        }
        return sets;
    }

    public void addCell(Cell cell) throws IOException {
        walWriter.write(cell);
        current.put(cell);
        flushIfOK();
    }

    private void flushIfOK() throws IOException {
        if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {

                    MemCache tmpCache = current;
                    try {
                        LockContext.flushLock.writeLock().lock();
                        flushingList.add(tmpCache);
                        current = new MemCache(configuration);
                    }finally {
                        LockContext.flushLock.writeLock().unlock();
                    }
                    IWalWriter tmpWalWriter = walWriter;
                    walWriter.close();
                    walWriter.markFlush();
                    walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));

                    //todo:flush完毕后，从flushList里面删除
                    flusher.flush(new IFlusher.FlushEntry(tmpCache,tmpWalWriter,  () -> flushingList.remove(tmpCache)));
                }
            }
        }
    }
}
