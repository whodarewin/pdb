package com.hc.pdb;

/**
 * pdb interface
 */
public interface PDB {

    byte[] get(byte[] key);

    byte[] put(byte[] key,byte[] value);

    byte[] scan(byte[] start,byte[] end);

    byte[] delete(byte[] key);
}
