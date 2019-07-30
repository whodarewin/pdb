package com.hc.pdb.flusher;

import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.CrashWorkerManager;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.NamedThreadFactory;

import java.io.IOException;
import java.util.concurrent.*;

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


    private ThreadPoolExecutor executor;
    private HCCWriter hccWriter;
    private StateManager manager;
    private CrashWorkerManager creashWorkerManager;
    private String path;

    public Flusher(String path,int flushThreadNum,
                   HCCWriter hccWriter, StateManager manager,
                   CrashWorkerManager creashWorkerManager) {

        this.path = path;
        executor = new ThreadPoolExecutor(flushThreadNum, flushThreadNum,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("pdb-flusher"));
        this.hccWriter = hccWriter;
        this.manager = manager;
        this.creashWorkerManager = creashWorkerManager;

    }

    @Override
    public Future<Boolean> flush(FlushEntry entry) throws Exception {
        Future<Boolean> ret = creashWorkerManager.doWork(
                new FlusherCrashable(path,entry,hccWriter,manager),
                executor);
        return ret;
    }

    @Override
    public int getWaitingToFlushSize() {
        return executor.getQueue().size();
    }

}
