package com.hc.pdb.flusher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBStopException;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.state.WALFileMeta;
import com.hc.pdb.util.PDBFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
    private String walPath;

    public Flusher(FlushEntry entry, HCCWriter writer,
                   StateManager manager, PDBStatus pdbStatus) {
        Preconditions.checkNotNull(entry, "FlushEntry can not be null");
        Preconditions.checkNotNull(writer, "hccWriter can not be null");
        Preconditions.checkNotNull(manager,"state manager can not be null");
        Preconditions.checkNotNull(pdbStatus,"PDBStatus can not be null");

        this.cache = entry.getMemCache();
        this.hccWriter = writer;
        this.stateManager = manager;
        this.callback = entry.getCallback();
        this.pdbStatus = pdbStatus;
        this.walFileMeta = entry.getWalFileMeta();
        this.hccFileName = walFileMeta.getParams().get(0).toString();
        this.walPath = walFileMeta.getWalPath();
    }

    @Override
    public void run() {
        try {
            if(pdbStatus.isClose()){
                LOGGER.info("pdb closed,flush exist");
                throw new PDBStopException();
            }
            LOGGER.info("begin flush,wal file path {},hcc file path is {}",walPath,walFileMeta.getParams().get(0));
            switch (walFileMeta.getState()){
                case WALFileMeta.BEGIN_FLUSH :
                    dealWithHCCBeginFlush();
                    break;
                case WALFileMeta.HCC_WRITE_FINISH :
                    dealWithHCCWriteFinish();
                    break;
                case WALFileMeta.HCC_CHANGE_NAME_FINISH:
                    dealWithHCCChangeNameFinish();
                    break;
                default:
                    throw new FlushException("no state " + walFileMeta.getState() + " found");
            }
        } catch (Exception e) {
            pdbStatus.setClosed("flush exception");
            pdbStatus.setCrashException(e);
        }
    }

    private void dealWithHCCBeginFlush() throws Exception {
        if(pdbStatus.isClose()){
            LOGGER.info("pdb closed,flush exist");
            throw new PDBStopException();
        }
        deleteHccFileFirst();
        HCCFileMeta meta = flushHCCFile();
        if(meta == null){
            LOGGER.info("meta is null,check if pdb is closed,stop flush");
            return;
        }
        String fileName = changeHccFileName();
        if(StringUtils.isNotEmpty(fileName)){
            meta.setFilePath(fileName);
        }else{
            throw new PDBException("change flush fileName error,"+meta.getFilePath());
        }
        changeMetaAndDeleteWalFile(meta);
        stateManager.deleteFlushingWal(walPath);
    }

    private void dealWithHCCWriteFinish() throws Exception {
        if(pdbStatus.isClose()){
            LOGGER.info("pdb closed,flush exist");
            throw new PDBStopException();
        }
        HCCFileMeta meta = (HCCFileMeta) walFileMeta.getParams().get(1);
        String fileName = changeHccFileName();
        if(StringUtils.isNotEmpty(fileName)){
            meta.setFilePath(fileName);
        }else{
            throw new PDBException("change flush fileName error," + meta.getFilePath());
        }
        changeMetaAndDeleteWalFile(meta);
        stateManager.deleteFlushingWal(walPath);
    }

    private void dealWithHCCChangeNameFinish() throws Exception {
        if(pdbStatus.isClose()){
            LOGGER.info("pdb closed,flush exist");
            throw new PDBStopException();
        }
        changeMetaAndDeleteWalFile((HCCFileMeta) walFileMeta.getParams().get(1));
        stateManager.deleteFlushingWal(walPath);
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
        stateManager.addFlushingWal(walPath,WALFileMeta.HCC_WRITE_FINISH, Lists.newArrayList(hccFileName,meta));
        return meta;
    }

    private String changeHccFileName(){

        String hccFileNameWithoutFlush = this.hccFileName.substring(0, this.hccFileName.length() -
                FileConstants.DATA_FILE_FLUSH_SUFFIX.length());
        File hccFile = new File(hccFileName);
        File hccFileWithoutFlush = new File(hccFileNameWithoutFlush);
        if(hccFile.exists()){
            hccFile.renameTo(hccFileWithoutFlush);
            return hccFileNameWithoutFlush;
        }else if(!hccFile.exists() && hccFileWithoutFlush.exists()){
            return hccFileNameWithoutFlush;
        }
        return null;
    }

    private void changeMetaAndDeleteWalFile(HCCFileMeta meta) throws Exception {
        if(meta != null){
            return;
        }
        try {
            LockContext.FLUSH_LOCK.writeLock().lock();
            //hcc manager有了
            stateManager.add(meta);
            //删除flush的
            callback.callback();
        } finally {
            LockContext.FLUSH_LOCK.writeLock().unlock();
        }
        File walFile = new File(walPath);
        if(walFile.exists()){
            FileUtils.forceDelete(walFile);
        }
        stateManager.addFlushingWal(walPath,WALFileMeta.CHANGE_META_DELETE_WAL_FINISH,
                Lists.newArrayList(walPath));
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
