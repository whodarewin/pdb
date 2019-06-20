package com.hc.pdb.file;

public class FileConstants {
    /**
     * 数据文件后缀
     */
    public static final String DATA_FILE_SUFFIX = ".hcc";
    /**
     * wal 文件后缀
     */
    public static final String WAL_FILE_SUFFIX = ".wal";
    /**
     * 元信息文件后缀
     */
    public static final String META_FILE_SUFFIX = ".meta";
    /**
     * 数据文件开始验证字节
     */
    public static final byte[] HCC_WRITE_PREFIX = new byte[]{72, 67, 67};
}
