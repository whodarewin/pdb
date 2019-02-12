package com.hc.pdb;

import java.util.Iterator;

/**
 * pdb interface
 */
public interface PDB {

    byte[] get(byte[] key);

    void put(byte[] key,byte[] value);

    Iterator<Cell> scan(byte[] start, byte[] end);

    void delete(byte[] key);
}
