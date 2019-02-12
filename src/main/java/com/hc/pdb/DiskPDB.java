package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.engine.Engine;

import java.util.Iterator;

/**
 * 基于disk的db
 */
public class DiskPDB implements PDB {

    private Configuration configuration;
    private Engine engine;


    public DiskPDB(Configuration configuration) {
        this.configuration = configuration;
        this.engine = new Engine(configuration);
    }

    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public void put(byte[] key, byte[] value) {
        this.engine.put(key,value,-1);
    }

    @Override
    public Iterator<Cell> scan(byte[] start, byte[] end) {
        return null;
    }

    @Override
    public void delete(byte[] key) {

    }
}
