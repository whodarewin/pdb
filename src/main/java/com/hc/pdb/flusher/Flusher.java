package com.hc.pdb.flusher;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.util.NamedThreadFactory;
import com.hc.pdb.wal.IWalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private HCCWriter hccWriter;

    public Flusher(Configuration configuration, HCCWriter hccWriter) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        int flushThreadNum = configuration.getInt(PDBConstants.FLUSHER_THREAD_SIZE_KEY,
                PDBConstants.DEFAULT_FLUSHER_THREAD_SIZE);
        executor = new ThreadPoolExecutor(flushThreadNum, flushThreadNum,
                0L, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new NamedThreadFactory("pdb-flusher"));
        this.hccWriter = hccWriter;
    }

    @Override
    public Future<Boolean> flush(FlushEntry entry) {
        Future<Boolean> ret = executor.submit(new FlushWorker(entry, hccWriter));
        return ret;
    }

    @Override
    public int getWaitingToFlushSize() {
        return executor.getQueue().size();
    }

    public static class FlushWorker implements Callable<Boolean> {
        private static final Logger LOGGER = LoggerFactory.getLogger(FlushWorker.class);
        private MemCache cache;
        private HCCWriter hccWriter;
        private IWalWriter walWriter;

        public FlushWorker(FlushEntry entry,HCCWriter writer) {
            Preconditions.checkNotNull(entry.getMemCache(), "MemCache can not be null");
            Preconditions.checkNotNull(entry.getWalWriter(),"WalWriter can not be null");
            Preconditions.checkNotNull(writer, "hccWriter can not be null");
            this.cache = entry.getMemCache();
            this.hccWriter = writer;
            this.walWriter = entry.getWalWriter();
        }

        @Override
        public Boolean call() {
            try {
                List<Cell> cells = new ArrayList<>(cache.getAllCells());
                hccWriter.writeHCC(cells);
                walWriter.delete();
                return true;
            } catch (Exception e) {
                LOGGER.error("flush error", e);
                return false;
            }

        }
    }
}
