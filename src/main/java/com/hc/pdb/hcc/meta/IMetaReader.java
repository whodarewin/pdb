package com.hc.pdb.hcc.meta;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * read meta 信息
 */
public interface IMetaReader {
    MetaInfo read(RandomAccessFile randomAccessFile) throws IOException;
}
