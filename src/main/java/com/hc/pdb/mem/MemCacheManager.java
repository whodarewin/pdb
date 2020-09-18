package com.hc.pdb.mem;

import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.NoEnoughByteException;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.flusher.Flusher;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.*;
import com.hc.pdb.util.NamedThreadFactory;
import com.hc.pdb.util.PDBFileUtils;
import com.hc.pdb.util.RangeUtil;
import com.hc.pdb.wal.FileWalWriter;
import com.hc.pdb.wal.IWalWriter;
import com.hc.pdb.wal.WalFileReader;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * mem cache的manager
 * @author congcong.han
 * @date 2019/6/22
 */
public class MemCacheManager implements IRecoveryable, PDBStatus.StatusListener{
    private static final Logger LOGGER = LoggerFactory.getLogger(MemCacheManager.class);
    private static final String FLUSHER = "flusher";

    private StateManager stateManager;
    private Configuration configuration;
    private List<Flusher.FlushEntry> flushingEntry = Collections.synchronizedList(new ArrayList<>());
    private MemCache current;
    private HCCWriter hccWriter;
    private IWalWriter walWriter;
    private ExecutorService flushExecutor;
    private PDBStatus pdbStatus;
    private String path;

    public MemCacheManager(Configuration configuration,
                           StateManager manager,
                           HCCWriter hccWriter,
                           PDBStatus pdbStatus) throws Exception {
        this.configuration = configuration;
        this.stateManager = manager;
        this.hccWriter = hccWriter;
        this.pdbStatus = pdbStatus;

        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        int flushThreadNum = configuration.getInt(PDBConstants.FLUSHER_THREAD_SIZE_KEY,
                PDBConstants.DEFAULT_FLUSHER_THREAD_SIZE);
        flushExecutor = new ThreadPoolExecutor(flushThreadNum, flushThreadNum,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("pdb-flusher"));
        recovery();
    }

    public Set<MemCache> searchMemCache(byte[] startKey, byte[] endKey){
        Set<MemCache> sets =  flushingEntry.stream().filter(cache ->
                RangeUtil.inOpenCloseInterval(cache.getMemCache().getStart(),cache.getMemCache().getEnd(),startKey,endKey))
                .map(flushEntry -> flushEntry.getMemCache())
                .collect(Collectors.toSet());
        if(!this.current.isEmpty() && RangeUtil.inOpenCloseInterval(current.getStart(),current.getEnd(),startKey,endKey)){
            sets.add(current);
        }
        return sets;
    }

    public void addCell(Cell cell) throws PDBException {
        walWriter.write(cell);
        current.put(cell);
        flushIfOK();
    }

    private void flushIfOK() throws PDBException {
        if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
            synchronized (this) {
                if (current.size() > configuration.getLong(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,
                        PDBConstants.DEFAULT_MEM_CACHE_MAX_SIZE)) {
                    if(current.iterator(null,null).next() == null){
                        LOGGER.warn("no cell found");
                        initCurrentWalAndMemCache();
                        return;
                    }
                    MemCache tmpCache = current;
                    Flusher.FlushEntry entry;
                    String hccFileName = PDBFileUtils.createHccFileFlushName(path);
                    //1 记录日志
                    WALFileMeta walFileMeta =
                            new WALFileMeta(walWriter.getWalFileName(),WALFileMeta.CREATE,Lists.newArrayList(hccFileName));
                    stateManager.addFlushingWal(walFileMeta);
                    walFileMeta.setState(WALFileMeta.BEGIN_FLUSH);
                    //2 关闭老的walWriter
                    IWalWriter tmpWalWriter = walWriter;
                    tmpWalWriter.close();
                    entry = new Flusher.FlushEntry(tmpCache,walFileMeta,
                            () -> flushingEntry.removeIf(
                                    flushEntry -> flushEntry.getMemCache().getId().equals(tmpCache.getId()))
                    );
                    try {
                        LockContext.FLUSH_LOCK.writeLock().lock();
                        flushingEntry.add(entry);
                        //3 创建新的一套写入系统
                        initCurrentWalAndMemCache();

                    }finally {
                        LockContext.FLUSH_LOCK.writeLock().unlock();
                    }
                    stateManager.addFlushingWal(walWriter.getWalFileName(),WALFileMeta.BEGIN_FLUSH,Lists.newArrayList(hccFileName));
                    flushExecutor.submit(new Flusher(entry,hccWriter,stateManager,pdbStatus));
                }
            }
        }
    }

    @Override
    public void onClose() throws PDBIOException, InterruptedException {
        this.flushExecutor.shutdownNow();

        while(!this.flushExecutor.awaitTermination(1,TimeUnit.SECONDS)){
            LOGGER.info("wait flush terminated...");
        }

        this.walWriter.close();
    }

    @Override
    public void recovery()throws RecorverFailedException{

        try {
            //没有需要recorver的,即刚启动的那种情况
            if(recoveryIfNone()){
                return;
            }

            //1. 整理meta
            //1. CREATE了但是没有将currentWal重置的
            Set<WALFileMeta> metas = new HashSet<>();
            metas.addAll(stateManager.getFlushingWal());
            for (WALFileMeta walFileMeta : metas) {
                if (WALFileMeta.CREATE.equals(walFileMeta.getState())) {
                    if(walFileMeta.getWalPath().equals(stateManager.getCurrentWALFileMeta().getWalPath())){
                        initCurrentWalAndMemCache();
                    }
                    stateManager.addFlushingWal(walFileMeta.getWalPath(),WALFileMeta.BEGIN_FLUSH,walFileMeta.getParams());
                }
            }
            //2. 处理currentWal
            WALFileMeta current = stateManager.getCurrentWALFileMeta();

            WalFileReader reader = new WalFileReader(current.getWalPath());
            try {
                if (reader.read().hasNext()) {
                   //flush
                    reader.close();
                    String hccFileName = PDBFileUtils.createHccFileName(path);
                    stateManager.addFlushingWal(current.getWalPath(),WALFileMeta.CREATE,Lists.newArrayList(hccFileName));
                    initCurrentWalAndMemCache();
                    stateManager.addFlushingWal(current.getWalPath(),WALFileMeta.BEGIN_FLUSH,Lists.newArrayList(hccFileName));
                }else{
                    initCurrentWalAndMemCache(current.getWalPath());
                }
            }catch (NoEnoughByteException e){
                //drop and init new one
                initCurrentWalAndMemCache();
            }

            for(WALFileMeta walFileMeta : stateManager.getFlushingWal()){
                MemCache cache = new MemCache();
                cache.init(new WalFileReader(walFileMeta.getWalPath()));
                Flusher.FlushEntry entry = new Flusher.FlushEntry(cache,
                        walFileMeta,
                        () -> flushingEntry.removeIf(
                                flushEntry -> flushEntry.getMemCache().getId().equals(cache.getId())));
                this.flushingEntry.add(entry);

                flushExecutor.submit(new Flusher(entry,hccWriter,stateManager,pdbStatus));
            }

        }catch (Exception e){
            throw new RecorverFailedException(e);
        }
    }



    private boolean recoveryIfNone() throws Exception {
        if (CollectionUtils.isEmpty(stateManager.getFlushingWal())
                && stateManager.getCurrentWALFileMeta() == null) {
            initCurrentWalAndMemCache();
            return true;
        }
        return false;
    }

    private void initCurrentWalAndMemCache() throws PDBException {
        String walFileName = PDBFileUtils.createWalFileName(path);
        this.initCurrentWalAndMemCache(walFileName);
    }

    private void initCurrentWalAndMemCache(String walFileName) throws PDBException {
        stateManager.setCurrentWalFileMeta(new WALFileMeta(walFileName,null,Lists.newArrayList(walFileName)));
        this.current = new MemCache();
        this.walWriter = new FileWalWriter(walFileName);
    }
}
