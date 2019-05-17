package com.hc.pdb.flusher;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.mem.MemCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class Flusher implements IFlusher {

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private HCCWriter hccWriter;

    public Flusher(Configuration configuration, HCCWriter hccWriter) {
        Preconditions.checkNotNull(configuration);
        this.configuration = configuration;
        int flushThreadNum = configuration.getInt(Constants.FLUSHER_THREAD_SIZE_KEY, Constants.DEFAULT_FLUSHER_THREAD_SIZE);
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(flushThreadNum);
        this.hccWriter = hccWriter;
    }

    @Override
    public Future<Boolean> flush(MemCache cache) {
        Future<Boolean> ret = executor.submit(new FlushWorker(cache, hccWriter));
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

        public FlushWorker(MemCache cache, HCCWriter hccWriter) {
            Preconditions.checkNotNull(cache, "MemCache can not be null");
            Preconditions.checkNotNull(hccWriter, "hccWriter can not be null");
            this.cache = cache;
            this.hccWriter = hccWriter;
        }

        @Override
        public Boolean call() {
            try {
                List<Cell> cells = new ArrayList<>(cache.getAllCells());
                hccWriter.writeHCC(cells);
                return true;
            } catch (Exception e) {
                LOGGER.error("flush error", e);
                return false;
            }

        }
    }
}
