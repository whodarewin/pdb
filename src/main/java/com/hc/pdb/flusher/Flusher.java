package com.hc.pdb.flusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.CreashWorkerManager;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.NamedThreadFactory;
import com.hc.pdb.wal.IWalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;


/**
 * Flusher
 * flush memCache 到磁盘
 * 流程：
 * 1. 如果MemCache达到了限定的大小，则flush到磁盘
 * 2. 如果所有的flush线程全部在用，则阻塞写入。
 * @author han.congcong
 * @date 2019/6/3
 */

public class Flusher implements IFlusher {

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private HCCWriter hccWriter;
    private StateManager manager;
    private CreashWorkerManager creashWorkerManager;
    private String path;

    public Flusher(Configuration configuration, HCCWriter hccWriter, StateManager manager,
                   CreashWorkerManager creashWorkerManager) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        int flushThreadNum = configuration.getInt(PDBConstants.FLUSHER_THREAD_SIZE_KEY,
                PDBConstants.DEFAULT_FLUSHER_THREAD_SIZE);
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        executor = new ThreadPoolExecutor(flushThreadNum, flushThreadNum,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("pdb-flusher"));
        this.hccWriter = hccWriter;
        this.manager = manager;
        this.creashWorkerManager = creashWorkerManager;
    }

    @Override
    public Future<Boolean> flush(FlushEntry entry) throws JsonProcessingException {
        List<String> param = Lists.newArrayList(entry.getWalWriter().getWalFileName());
        Future<Boolean> ret = creashWorkerManager.doWork(new FlusherCrashable(
                path,entry,hccWriter,manager
        ),param,executor);
        return ret;
    }

    @Override
    public int getWaitingToFlushSize() {
        return executor.getQueue().size();
    }

}
