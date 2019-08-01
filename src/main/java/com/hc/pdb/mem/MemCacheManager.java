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

    private IWalWriter walWriter;
    private StateManager stateManager;
    private Configuration configuration;
    private List<MemCache> flushingList = Collections.synchronizedList(new ArrayList<>());
    private MemCache current;
    private HCCWriter hccWriter;
    private CrashWorkerManager crashWorkerManager;

    private ExecutorService flushExecutor;
    private String path;

    public MemCacheManager(Configuration configuration,
                           StateManager manager,
                           HCCWriter hccWriter,
                           CrashWorkerManager crashWorkerManager) throws IOException {
        this.stateManager = manager;
        flushIfHaveRemainedWal();
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
    }

    private void flushIfHaveRemainedWal() throws IOException {
        WALFileMeta walFileMeta = stateManager.getState().getWalFileMeta();
        if(walFileMeta != null){
            String walPath = walFileMeta.getWalPath();
            WalFileReader reader = new WalFileReader(walPath);
            MemCache cache = new MemCache(reader);
            //todo:flush
        }
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
                    try {
                        LockContext.flushLock.writeLock().lock();
                        flushingList.add(tmpCache);
                        current = new MemCache();
                    }finally {
                        LockContext.flushLock.writeLock().unlock();
                    }
                    IWalWriter tmpWalWriter = walWriter;
                    walWriter.close();
                    walWriter.markFlush();
                    walWriter = new DefaultWalWriter(configuration.get(PDBConstants.DB_PATH_KEY));

                    //todo:flush完毕后，从flushList里面删除
                    Future<Boolean> ret = crashWorkerManager.doWork(
                            new FlusherCrashable(path,new FlusherCrashable.FlushEntry(tmpCache,tmpWalWriter,
                                    () -> flushingList.remove(tmpCache))
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
    public IWorkerCrashable create(Recorder.RecordLog log) {
        return null;
    }
}
