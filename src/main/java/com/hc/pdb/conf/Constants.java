package com.hc.pdb.conf;

public class Constants {
    /**
     * 单位kb
     */
    public static final long BLOCK_SIZE = 1024;

    public static final String BLOCK_SIZE_KEY = "block.size.key";

    public static final String DB_PATH_KEY = "db.path.key";

    public static final String ERROR_RATE_KEY = "error.rate.key";

    public static final Double DEFAULT_ERROR_RATE = 0.0001;

    public static final String FLUSHER_THREAD_SIZE_KEY = "flusher.thread.size.key";

    public static final int DEFAULT_FLUSHER_THREAD_SIZE = 8;

    public static final String MEM_CACHE_MAX_SIZE_KEY = "mem.cache.max.size.key";

    public static final long DEFAULT_MEM_CACHE_MAX_SIZE = 1024 * 1024 * 100;
}
