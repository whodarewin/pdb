package com.hc.pdb;

/**
 * 基于disk的db
 */
public class DiskPDB implements PDB {

    private String path;

    public DiskPDB(String path) {
        this.path = path;
    }


    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public byte[] put(byte[] key, byte[] value) {
        return new byte[0];
    }

    @Override
    public byte[] scan(byte[] start, byte[] end) {
        return new byte[0];
    }

    @Override
    public byte[] delete(byte[] key) {
        return new byte[0];
    }
}
