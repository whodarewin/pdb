package com.hc.pdb.hcc.meta;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface IMetaReader {
    MetaInfo read(RandomAccessFile randomAccessFile) throws IOException;
}
