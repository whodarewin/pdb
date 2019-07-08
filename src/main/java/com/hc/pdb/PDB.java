package com.hc.pdb;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.engine.IEngine;
import com.hc.pdb.engine.LSMEngine;
import com.hc.pdb.scanner.IScanner;

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

    public byte[] get(byte[] key) throws IOException {
        Preconditions.checkNotNull(key,"key can not be null");
        return engine.get(key);
    }

    public void put(byte[] key, byte[] value) throws IOException {
        this.engine.put(key, value, -1);
    }

    public IScanner scan(byte[] start, byte[] end) throws IOException {
        return this.engine.scan(start,end);
    }

    public void delete(byte[] key) throws IOException {
        this.engine.delete(key);
    }

    public void close(){
        this.engine.close();
    }

    public void clean() throws IOException {
        this.engine.clean();
    }
}
