package com.hc.pdb.mem;

import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
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

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * mem cache的manager
 * @author congcong.han
 * @date 2019/6/22
 */
public class MemCacheManager implements IWorkerCrashableFactory, PDBStatus.StatusListener {
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
                           CrashWorkerManager crashWorkerManager) throws Exception {
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
        crashWorkerManager.register(this);
        restoreFlushingMemCache();
        stateManager.setCurrentWalFileMeta(new WALFileMeta(walWriter.getWalFileName(),false));
    }

    private void restoreFlushingMemCache() throws Exception {

        Collection<WALFileMeta> fileMetaList = stateManager.getFlushingWal();

        for (WALFileMeta meta : fileMetaList){
            if (meta != null) {
                String walPath = meta.getWalPath();
                WalFileReader reader = new WalFileReader(walPath);
                MemCache cache = new MemCache(reader);
                this.flushingEntry.add(new FlusherCrashable.FlushEntry(cache,
                        walPath,
                        () -> finishFlush(cache, walPath)));
            }
        }
        WALFileMeta walFileMeta = stateManager.getState().getWalFileMeta();

        if(walFileMeta != null){
            stateManager.getFlushingWal().add(walFileMeta);
            MemCache cache = new MemCache(new WalFileReader(walFileMeta.getWalPath()));
            FlusherCrashable.FlushEntry entry = new FlusherCrashable.FlushEntry(
                    cache,
                    walFileMeta.getWalPath(),
                    () -> flushingEntry.removeIf(flushEntry -> flushEntry.getMemCache().getId().equals(cache.getId()))

            );
            flushingEntry.add(entry);
            crashWorkerManager.doWork(
                    new FlusherCrashable(
                            path,
                            entry,
                            hccWriter,
                            stateManager),
                    flushExecutor);
        }
    }

    public void finishFlush(MemCache cache,String walPath) throws Exception {
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

                        stateManager.addFlushingWal(walWriter.getWalFileName());

                        IWalWriter tmpWalWriter = walWriter;
                        walWriter.close();
                        walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));
                        entry = new FlusherCrashable.FlushEntry(tmpCache,tmpWalWriter.getWalFileName(),
                                () -> flushingEntry.removeIf(
                                        flushEntry -> flushEntry.getMemCache().getId().equals(tmpCache.getId()))
                        );
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
    public IWorkerCrashable create(List<Recorder.RecordLog> log) {
        Recorder.RecordLog initLog = log.stream()
                .filter(log1 -> FlusherCrashable.PRE_RECORD.equals(log1.getProcessStage()))
                .collect(Collectors.toList())
                .get(0);
        String walPath = initLog.getParams().get(0);
        FlusherCrashable.FlushEntry flushEntry = flushingEntry.stream()
                .filter(flushEntry1 -> flushEntry1.getWalPath().equals(walPath))
                .findFirst().get();
        return new FlusherCrashable(path,flushEntry,hccWriter,stateManager);
    }

    @Override
    public void onClose() {
        this.flushExecutor.shutdown();
    }
}
