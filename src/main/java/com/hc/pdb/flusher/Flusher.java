package com.hc.pdb.flusher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBSerializeException;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.state.WALFileMeta;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author han.congcong
 * @date 2019/7/27
 */
public class Flusher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

    private MemCache cache;
    private HCCWriter hccWriter;
    private StateManager stateManager;
    private Callback callback;
    private PDBStatus pdbStatus;
    private WALFileMeta walFileMeta;
    private String hccFileName;


    public Flusher(FlushEntry entry, HCCWriter writer,
                   StateManager manager, PDBStatus pdbStatus) {
        Preconditions.checkNotNull(entry.getMemCache(), "MemCache can not be null");
        Preconditions.checkNotNull(entry.getWalFileMeta(),"WalFileMeta can not be null");
        Preconditions.checkNotNull(writer, "hccWriter can not be null");
        Preconditions.checkNotNull(manager,"state manager can not be null");

        this.cache = entry.getMemCache();
        this.hccWriter = writer;
        this.stateManager = manager;
        this.callback = entry.getCallback();
        this.pdbStatus = pdbStatus;
        this.walFileMeta = entry.getWalFileMeta();
        this.hccFileName = walFileMeta.getParams().get(0).toString();
    }

    @Override
    public void run() {
        try {
            LOGGER.info("begin flush,wal file path {},hcc file path is {}",walFileMeta.getWalPath(),walFileMeta.getParams().get(0));
            switch (walFileMeta.getState()){
                case WALFileMeta.BEGIN_FLUSH :
                    deleteHccFileFirst();
                    HCCFileMeta meta = flushHCCFile();
                    changeMetaAndDeleteWalFile(meta);
                    stateManager.deleteFlushingWal(walFileMeta.getWalPath());
                    break;
                case WALFileMeta.HCC_WRITE_FINISH:
                    changeMetaAndDeleteWalFile((HCCFileMeta) walFileMeta.getParams().get(1));
                    stateManager.deleteFlushingWal(walFileMeta.getWalPath());
                    break;
                default:
                    throw new FlushException("no state " + walFileMeta.getState() + " found");
            }
        } catch (Exception e) {
            pdbStatus.setClose(true,"flush exception");
            pdbStatus.setCrashException(e);
        }
    }

    private void deleteHccFileFirst() throws IOException {
        File hccFile = new File(hccFileName);
        if(hccFile.exists()){
            FileUtils.forceDelete(hccFile);
        }
    }

    private HCCFileMeta flushHCCFile() throws PDBException {
        List<Cell> cells = new ArrayList<>(cache.getAllCells());
        HCCFileMeta meta = hccWriter.writeHCC(cells.iterator(), cells.size(), hccFileName);
        stateManager.addFlushingWal(walFileMeta.getWalPath(),WALFileMeta.HCC_WRITE_FINISH, Lists.newArrayList(hccFileName,meta));
        return meta;
    }

    private void changeMetaAndDeleteWalFile(HCCFileMeta meta) throws Exception {
        if(meta != null){
            return;
        }
        try {
            LockContext.flushLock.writeLock().lock();
            //hcc manager有了
            stateManager.add(meta);
            //删除flush的
            callback.callback();
        } finally {
            LockContext.flushLock.writeLock().unlock();
        }
        File walFile = new File(walFileMeta.getWalPath());
        if(walFile.exists()){
            FileUtils.forceDelete(walFile);
        }
        stateManager.addFlushingWal(walFileMeta.getWalPath(),WALFileMeta.CHANGE_META_DELETE_WAL_FINISH,
                Lists.newArrayList(walFileMeta.getWalPath()));
    }



    public interface Callback{
        void callback() throws Exception;
    }

    public static class FlushEntry{

        private MemCache memCache;
        private WALFileMeta walFileMeta;
        private Callback callback;



        public FlushEntry(MemCache memCache, WALFileMeta walFileMeta, Callback callback) {
            this.memCache = memCache;
            this.walFileMeta = walFileMeta;
            this.callback = callback;
        }

        public MemCache getMemCache() {
            return memCache;
        }

        public void setMemCache(MemCache memCache) {
            this.memCache = memCache;
        }

        public Callback getCallback() {
            return callback;
        }

        public void setCallback(Callback callback) {
            this.callback = callback;
        }

        public WALFileMeta getWalFileMeta() {
            return walFileMeta;
        }

        public void setWalFileMeta(WALFileMeta walFileMeta) {
            this.walFileMeta = walFileMeta;
        }
    }

}
