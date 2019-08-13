package com.hc.pdb.mem;

import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.ISafeClose;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
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
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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
                    if(current.iterator(null,null).next() == null){
                        LOGGER.warn("no cell found");
                        initCurrentWalAndMemCache();
                        return;
                    }
                    MemCache tmpCache = current;
                    Flusher.FlushEntry entry;
                    String hccFileName = PDBFileUtils.createHccFileName(path);
                    //1 记录日志
                    stateManager.addFlushingWal(walWriter.getWalFileName(),WALFileMeta.BEGIN_FLUSH,Lists.newArrayList(hccFileName));
                    //2 关闭老的walWriter
                    IWalWriter tmpWalWriter = walWriter;
                    tmpWalWriter.close();
                    try {
                        LockContext.flushLock.writeLock().lock();

                        entry = new Flusher.FlushEntry(tmpCache,tmpWalWriter.getWalFileName(),
                                () -> flushingEntry.removeIf(
                                        flushEntry -> flushEntry.getMemCache().getId().equals(tmpCache.getId()))
                        );
                        flushingEntry.add(entry);
                        //3 创建新的一套写入系统
                        initCurrentWalAndMemCache();

                    }finally {
                        LockContext.flushLock.writeLock().unlock();
                    }
                    flushExecutor.submit(new Flusher(hccFileName,entry,hccWriter,stateManager,pdbStatus));
                }
            }
        }
    }

    @Override
    public void onClose() throws IOException {
        this.flushExecutor.shutdownNow();
        this.walWriter.close();
    }

    @Override
    public void recovery() throws RecorverFailedException {
        try {
            //没有需要recorver的
            if(recoveryIfNone()){
                return;
            }
            //1 wal 到flushing wal的修正
            recoveryMovingWALMeta();
            //2 将flushing的wal继续工作
            recoveryFlushingWal();
            //3 将currentWal flush
            flushCurrentWALIfHave();
        }catch(Exception e){
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

    private void recoveryMovingWALMeta() throws Exception {
        if(stateManager.getFlushingWal().contains(stateManager.getCurrentWALFileMeta())){
            stateManager.setCurrentWalFileMeta(null);
        }
    }

    private void recoveryFlushingWal() throws IOException {
        Collection<WALFileMeta> flushingWals = stateManager.getFlushingWal();
        for (WALFileMeta flushingWal : flushingWals) {
            String walPath = flushingWal.getWalPath();
            String state = flushingWal.getState();
            List<String> params = flushingWal.getParams();
            String hccFilePath = params.get(0);
            if(WALFileMeta.BEGIN_FLUSH.equals(state)){
                //1 删除hccFile
                FileUtils.forceDelete(new File(hccFilePath));
                //2 重新flush
                MemCache cache = new MemCache(new WalFileReader(walPath));
                Flusher.FlushEntry entry = new Flusher.FlushEntry(
                        cache,
                        walPath,
                        () -> flushingEntry.removeIf(
                                flushEntry -> flushEntry.getMemCache().getId().equals(cache.getId()))
                );
                flushingEntry.add(entry);
                flushExecutor.submit(new Flusher(hccFilePath,entry,hccWriter,stateManager,pdbStatus));
            }
        }
    }

    private void flushCurrentWALIfHave() throws Exception {

        WALFileMeta meta = stateManager.getCurrentWALFileMeta();
        if(meta == null){
            initCurrentWalAndMemCache();
            return;
        }

        String hccFilePath = PDBFileUtils.createHccFileName(path);
        stateManager.addFlushingWal(meta.getWalPath(),
                WALFileMeta.BEGIN_FLUSH,
                Lists.newArrayList(hccFilePath));

        initCurrentWalAndMemCache();
        //flush
        MemCache cache = new MemCache(new WalFileReader(meta.getWalPath()));
        if(cache.isEmpty()){
            return;
        }

        Flusher.FlushEntry entry = new Flusher.FlushEntry(
                cache,
                meta.getWalPath(),
                () -> flushingEntry.removeIf(
                        flushEntry -> flushEntry.getMemCache().getId().equals(cache.getId()))
        );
        flushingEntry.add(entry);
        flushExecutor.submit(new Flusher(hccFilePath,entry,hccWriter,stateManager,pdbStatus));
    }

    private void initCurrentWalAndMemCache() throws Exception {
        String walFileName = PDBFileUtils.createWalFileName(path);
        stateManager.setCurrentWalFileMeta(new WALFileMeta(walFileName,WALFileMeta.CREATE,Lists.newArrayList(walFileName)));
        this.current = new MemCache();
        this.walWriter = new FileWalWriter(walFileName);
    }
}
