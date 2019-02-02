package com.hc.pdb.file;

public interface FileConstants {
    String DATA_FILE_SUFFIX = ".hcc";//数据文件后缀名
    String WAL_FILE_SUFFIX = ".wal";
    byte[] HCC_WRITE_PREFIX = "hcc".getBytes();
}
