package com.hc.pdb.flusher;

import com.hc.pdb.mem.MemCache;

import java.util.concurrent.Future;

/**
 * 将mem刷到磁盘
 */
public interface IFlusher {

    /**
     * 将cache进行flush,此方法在flush的entry里面如果太多的时候，会阻塞。
     *
     * @param cache
     * @return
     */
    Future<Boolean> flush(MemCache cache);

    /**
     * 获得正在进行flush的任务
     *
     * @return
     */
    int getWaitingToFlushSize();
}
