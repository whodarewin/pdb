package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.engine.IEngine;
import com.hc.pdb.engine.LSMEngine;

import java.io.IOException;
import java.util.Iterator;

/**
 * PDB 对外暴露接口
 */
public class PDB {

    private Configuration configuration;
    private IEngine engine;

    public PDB(Configuration configuration) throws IOException {
        this.configuration = configuration;
        this.engine = new LSMEngine(configuration);
    }

    public byte[] get(byte[] key) {
        return new byte[0];
    }

    public void put(byte[] key, byte[] value) throws IOException {
        this.engine.put(key, value, -1);
    }

    public Iterator<Cell> scan(byte[] start, byte[] end) {
        return null;
    }

    public void delete(byte[] key) throws IOException {
        this.engine.delete(key);
    }
}
