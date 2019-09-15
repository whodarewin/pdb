package com.hc.pdb.conf;

public class PDBConstants {
    /**
     * 单位kb
     * block的大小
     */
    public static final long DEFAULT_BLOCK_SIZE = 1024;

    /**
     * 设置block大小的key
     */
    public static final String BLOCK_SIZE_KEY = "block.size.key";

    /**
     * 设置db的path的key
     */
    public static final String DB_PATH_KEY = "db.path.key";

    /**
     * 布隆过滤器的错误率
     */
    public static final String ERROR_RATE_KEY = "error.rate.key";

    /**
     * bloom filter的error rate
     */
    public static final Double DEFAULT_ERROR_RATE = 0.0001;

    /**
     * flusher的线程数量的key
     */
    public static final String FLUSHER_THREAD_SIZE_KEY = "flusher.thread.size.key";

    /**
     * 默认的flusher的数量
     */
    public static final int DEFAULT_FLUSHER_THREAD_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * compactor线程数量的key
     */
    public static final String COMPACTOR_THREAD_SIZE_KEY = "compactor.thread.size.key";

    /**
     * 默认的compactor线程数量的值
     */
    public static final int COMPACTOR_THREAD_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * compactor 进行compact的阈值的key
     */
    public static final String COMPACTOR_HCCFILE_THRESHOLD_KEY = "compactor.hccfile.threshold.key";

    /**
     * compactor 进行compact的阈值
     */
    public static final int COMPACTOR_HCCFILE_THRESHOLD = 2;

    /**
     * memcache 大小的key
     */
    public static final String MEM_CACHE_MAX_SIZE_KEY = "mem.cache.max.size.key";

    /**
     * 默认memcache大小
     */
    public static final long DEFAULT_MEM_CACHE_MAX_SIZE = 1024 * 1024 * 100;


    public static final class Charset{
        public static String UTF_8 = "utf-8";
    }
}
