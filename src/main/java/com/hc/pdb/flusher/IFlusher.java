package com.hc.pdb.flusher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.wal.IWalWriter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

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
    Future<Boolean> flush(FlushEntry entry) throws Exception;

    /**
     * 获得正在进行flush的任务
     *
     * @return
     */
    int getWaitingToFlushSize();


    class FlushEntry{
        private MemCache memCache;
        private IWalWriter walWriter;
        private Callback callback;


        public FlushEntry(MemCache memCache, IWalWriter walWriter,Callback callback) {
            this.memCache = memCache;
            this.walWriter = walWriter;
            this.callback = callback;
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

        public Callback getCallback() {
            return callback;
        }

        public void setCallback(Callback callback) {
            this.callback = callback;
        }
    }

    interface Callback{
        void callback();
    }
}
