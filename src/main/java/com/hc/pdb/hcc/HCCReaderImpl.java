package com.hc.pdb.hcc;

import com.hc.pdb.file.FileConstants;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class HCCReaderImpl implements HCCReader {
    private String path;
    private RandomAccessFile randomAccessFile;
    private List<byte[]> index = new ArrayList<>();

    public HCCReaderImpl(String path) {
        this.path = path;
    }

    @Override
    public void open() throws IOException {
        RandomAccessFile file = new RandomAccessFile(path, "r");

    }

    @Override
    public void scan(byte[] start, byte[] ends) {

    }

    @Override
    public void close() {

    }
}
