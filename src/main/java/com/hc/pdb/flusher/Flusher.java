package com.hc.pdb.flusher;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.StateManager;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * @author han.congcong
 * @date 2019/7/27
 */
public class Flusher implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Flusher.class);

    private MemCache cache;
    private HCCWriter hccWriter;
    private String walPath;
    private StateManager stateManager;
    private Callback callback;
    private PDBStatus pdbStatus;
    private String hccFilePath;

    public Flusher(String hccFilePath,FlushEntry entry, HCCWriter writer,
                   StateManager manager, PDBStatus pdbStatus) {
        Preconditions.checkNotNull(entry.getMemCache(), "MemCache can not be null");
        Preconditions.checkNotNull(entry.getWalPath(),"WalWriter can not be null");
        Preconditions.checkNotNull(writer, "hccWriter can not be null");
        Preconditions.checkNotNull(manager,"state manager can not be null");
        Preconditions.checkNotNull(hccFilePath,"path can not be null");

        this.cache = entry.getMemCache();
        this.hccWriter = writer;
        this.walPath = entry.getWalPath();
        this.stateManager = manager;
        this.callback = entry.getCallback();
        this.hccFilePath = hccFilePath;
        this.pdbStatus = pdbStatus;
    }

    @Override
    public void run() {
        try {
            LOGGER.info("begin flush,wal file path {},hcc file path is {}",walPath,hccFilePath);
            doFlush(hccFilePath);
            stateManager.deleteFlushingWal(walPath);
        } catch (Exception e) {
            pdbStatus.setClose(true,"flush exception");
            pdbStatus.setCrashException(e);
        }
    }

    private void doFlush(String fileName) throws Exception {
        List<Cell> cells = new ArrayList<>(cache.getAllCells());
        HCCFileMeta fileMeta = hccWriter.writeHCC(cells.iterator(), cells.size(), fileName);

        try {
            LockContext.flushLock.writeLock().lock();
            //hcc manager有了
            stateManager.add(fileMeta);
            //删除flush的
            callback.callback();
        }finally {
            LockContext.flushLock.writeLock().unlock();
        }

        FileUtils.forceDelete(new File(walPath));
    }

    public interface Callback{
        void callback() throws Exception;
    }

    public static class FlushEntry{

        private MemCache memCache;
        private String walPath;
        private Callback callback;


        public FlushEntry(MemCache memCache, String walPath,Callback callback) {
            this.memCache = memCache;
            this.walPath = walPath;
            this.callback = callback;
        }

        public MemCache getMemCache() {
            return memCache;
        }

        public void setMemCache(MemCache memCache) {
            this.memCache = memCache;
        }

        public String getWalPath() {
            return walPath;
        }

        public void setWalPath(String walPath) {
            this.walPath = walPath;
        }

        public Callback getCallback() {
            return callback;
        }

        public void setCallback(Callback callback) {
            this.callback = callback;
        }
    }

}
