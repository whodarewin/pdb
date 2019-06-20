package com.hc.pdb.conf;

public class PDBConstants {
    /**
     * 单位kb
     * block的大小
     */
    public static final long DEFAULT_BLOCK_SIZE = 1024;

    public static final String BLOCK_SIZE_KEY = "block.size.key";

    /**
     * db 的path
     */
    public static final String DB_PATH_KEY = "db.path.key";

    /**
     * 布隆过滤器的错误率
     */
    public static final String ERROR_RATE_KEY = "error.rate.key";

    public static final Double DEFAULT_ERROR_RATE = 0.0001;

    /**
     * flusher的线程数量
     */
    public static final String FLUSHER_THREAD_SIZE_KEY = "flusher.thread.size.key";

    public static final int DEFAULT_FLUSHER_THREAD_SIZE = Runtime.getRuntime().availableProcessors();

    /**
     * memcache 的key
     */
    public static final String MEM_CACHE_MAX_SIZE_KEY = "mem.cache.max.size.key";

    public static final long DEFAULT_MEM_CACHE_MAX_SIZE = 1024 * 1024 * 100;
}
