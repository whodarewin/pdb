package com.hc.pdb.flusher;

import com.hc.pdb.mem.MemCache;
import com.hc.pdb.wal.IWalWriter;

import java.util.concurrent.Future;

/**
 * 将mem刷到磁盘
 */
public interface IFlusher {

    /**
     * 将cache进行flush,此方法在flush的entry里面如果太多的时候，会阻塞。
     *
     * @param entry
     * @return
     */
    Future<Boolean> flush(FlushEntry entry);

    /**
     * 获得正在进行flush的任务
     *
     * @return
     */
    int getWaitingToFlushSize();


    class FlushEntry{
        private MemCache memCache;
        private IWalWriter walWriter;

        public FlushEntry(MemCache memCache, IWalWriter walWriter) {
            this.memCache = memCache;
            this.walWriter = walWriter;
        }

        public MemCache getMemCache() {
            return memCache;
        }

        public void setMemCache(MemCache memCache) {
            this.memCache = memCache;
        }

        public IWalWriter getWalWriter() {
            return walWriter;
        }

        public void setWalWriter(IWalWriter walWriter) {
            this.walWriter = walWriter;
        }
    }
}
