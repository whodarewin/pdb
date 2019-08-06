package com.hc.pdb.flusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.IWorkerCrashable;
import com.hc.pdb.state.Recorder;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.PDBFileUtils;
import com.hc.pdb.wal.WalFileReader;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Created by congcong.han on 2019/7/27.
 */
public class FlusherCrashable implements IWorkerCrashable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlusherCrashable.class);
    public static final String FLUSHER = "flusher";
    public static final String PRE_RECORD = "pre_record";
    public static final String FLUSH_BEGIN = "flush_begin";
    public static final String FLUSH_END = "flush_end";
    public static final String CHANGE_METE_BEGIN = "change_meta_begin";
    public static final String ALL_FINISH = "all_finish";

    private MemCache cache;
    private HCCWriter hccWriter;
    private String walPath;
    private StateManager stateManager;
    private Callback callback;
    private String path;

    public FlusherCrashable(String path,FlushEntry entry, HCCWriter writer, StateManager manager) {
        Preconditions.checkNotNull(entry.getMemCache(), "MemCache can not be null");
        Preconditions.checkNotNull(entry.getWalPath(),"WalWriter can not be null");
        Preconditions.checkNotNull(writer, "hccWriter can not be null");
        Preconditions.checkNotNull(manager,"state manager can not be null");
        Preconditions.checkNotNull(path,"path can not be null");
        this.cache = entry.getMemCache();
        this.hccWriter = writer;
        this.walPath = entry.getWalPath();
        this.stateManager = manager;
        this.callback = entry.getCallback();
        this.path = path;
    }

    @Override
    public String getName() {
        return FLUSHER;
    }

    @Override
    public void recordConstructParam(Recorder recorder) throws JsonProcessingException {
        recorder.recordMsg(PRE_RECORD,Lists.newArrayList(walPath));
    }
    @Override
    public void doWork(Recorder recorder) {
        try {
            String fileName = PDBFileUtils.createHccFileName(path);
            List<String> beginParam = Lists.newArrayList(walPath,fileName);
            recorder.recordMsg(FLUSH_BEGIN, beginParam);
            doFlush(fileName);
            recorder.recordMsg(ALL_FINISH,null);
        } catch (Exception e) {
            PDBStatus.setClose(true);
            PDBStatus.setCrashException(e);
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

    @Override
    public void continueWork(Recorder recorder) throws Exception {
        Recorder.RecordLog log = recorder.getCurrent();
        String state = log.getProcessStage();
        List<String> params = log.getParams();
        if (state.equals(FLUSH_BEGIN)){
            File walFile = new File(params.get(0));
            File hccFile = new File(params.get(1));
            if(walFile.exists() && hccFile.exists()){
                FileUtils.deleteQuietly(hccFile);
                this.cache = new MemCache(new WalFileReader(params.get(0)));
                doFlush(params.get(1));
                recorder.recordMsg(ALL_FINISH,null);
            }
        }
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
