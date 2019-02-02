package com.hc.pdb.hcc.bloom;

import com.hc.pdb.util.ByteBloomFilter;

import java.io.RandomAccessFile;

public interface IBloomFilterReader {

    ByteBloomFilter read(RandomAccessFile file, long bloomStartIndex);
}
