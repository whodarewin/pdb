package com.hc.pdb.mem;

import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.flusher.FlusherCrashable;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.*;
import com.hc.pdb.util.NamedThreadFactory;
import com.hc.pdb.util.RangeUtil;
import com.hc.pdb.wal.DefaultWalWriter;
import com.hc.pdb.wal.IWalWriter;
import com.hc.pdb.wal.WalFileReader;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * mem cache的manager
 * @author congcong.han
 * @date 2019/6/22
 */
public class MemCacheManager implements IWorkerCrashableFactory {
    private static final String FLUSHER = "flusher";

    private StateManager stateManager;
    private Configuration configuration;
    private List<FlusherCrashable.FlushEntry> flushingEntry = Collections.synchronizedList(new ArrayList<>());
    private MemCache current;
    private HCCWriter hccWriter;
    private CrashWorkerManager crashWorkerManager;
    private IWalWriter walWriter;
    private ExecutorService flushExecutor;
    private String path;

    public MemCacheManager(Configuration configuration,
                           StateManager manager,
                           HCCWriter hccWriter,
                           CrashWorkerManager crashWorkerManager) throws IOException {
        this.stateManager = manager;
        this.hccWriter = hccWriter;
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        int flushThreadNum = configuration.getInt(PDBConstants.FLUSHER_THREAD_SIZE_KEY,
                PDBConstants.DEFAULT_FLUSHER_THREAD_SIZE);

        this.walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
        current = new MemCache();
        this.configuration = configuration;

        flushExecutor = new ThreadPoolExecutor(flushThreadNum, flushThreadNum,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("pdb-flusher"));
        this.crashWorkerManager = crashWorkerManager;
        crashWorkerManager.register(FLUSHER,this);
        restoreFlushingMemCache();
    }

    private void restoreFlushingMemCache() throws IOException {
        WALFileMeta walFileMeta = stateManager.getState().getWalFileMeta();
        if(walFileMeta != null){
            String walPath = walFileMeta.getWalPath();
            WalFileReader reader = new WalFileReader(walPath);
            MemCache cache = new MemCache(reader);
            this.flushingEntry.add(new FlusherCrashable.FlushEntry(cache,
                    new DefaultWalWriter(walPath),
                    () -> finishFlush(cache,walPath)));
        }
    }

    public void finishFlush(MemCache cache,String walPath) throws IOException {
        stateManager.deleteFlushingWal(walPath);
        flushingEntry.remove(cache);
    }

    public Set<MemCache> searchMemCache(byte[] startKey, byte[] endKey){
        Set<MemCache> sets =  flushingEntry.stream().filter(cache ->
                RangeUtil.inOpenCloseInterval(cache.getMemCache().getStart(),cache.getMemCache().getEnd(),startKey,endKey))
                .map(flushEntry -> flushEntry.getMemCache())
                .collect(Collectors.toSet());
        if(RangeUtil.inOpenCloseInterval(current.getStart(),current.getEnd(),startKey,endKey)){
            sets.add(current);
        }
        return sets;
    }

    public void addCell(Cell cell) throws Exception {
        walWriter.write(cell);
        current.put(cell);
        flushIfOK();
    }

    private void flushIfOK() throws Exception {
        if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {

                    MemCache tmpCache = current;
                    FlusherCrashable.FlushEntry entry;
                    try {
                        LockContext.flushLock.writeLock().lock();
                        IWalWriter tmpWalWriter = walWriter;
                        walWriter.close();
                        walWriter.markFlush();
                        walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
                        entry = new FlusherCrashable.FlushEntry(tmpCache,tmpWalWriter,
                                () -> flushingEntry.remove(tmpCache));
                        flushingEntry.add(entry);
                        current = new MemCache();
                    }finally {
                        LockContext.flushLock.writeLock().unlock();
                    }

                    //todo:flush完毕后，从flushList里面删除
                    Future<Boolean> ret = crashWorkerManager.doWork(
                            new FlusherCrashable(path,entry
                                    ,hccWriter,stateManager),
                            flushExecutor);
                }
            }
        }
    }

    @Override
    public String getName() {
        return FLUSHER;
    }

    @Override
    public IWorkerCrashable create(Recorder.RecordLog log) throws IOException {
        List<String> constructParams = log.getConstructParam();
        String walPath = constructParams.get(0);
        FlusherCrashable.FlushEntry flushEntry = flushingEntry.stream()
                .filter(flushEntry1 -> flushEntry1.getWalWriter().getWalFileName().equals(walPath))
                .findFirst().get();
        return new FlusherCrashable(path,flushEntry,hccWriter,stateManager);
    }
}
