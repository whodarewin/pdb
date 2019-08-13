package com.hc.pdb;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.engine.IEngine;
import com.hc.pdb.engine.LSMEngine;
import com.hc.pdb.exception.DBCloseException;
import com.hc.pdb.scanner.IScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * TODO:key-value null值处理
 * PDB 对外暴露接口
 */
public class PDB {

    private Configuration configuration;
    private IEngine engine;

    public PDB(Configuration configuration) throws Exception {
        this.configuration = configuration;
        this.engine = new LSMEngine(configuration);
    }

    public byte[] get(byte[] key) throws IOException, DBCloseException {
        Preconditions.checkNotNull(key,"key can not be null");
        return engine.get(key);
    }

    public void put(byte[] key, byte[] value) throws Exception {
        this.engine.put(key, value, -1);
    }

    public void put(byte[] key, byte[] value, long ttl, TimeUnit unit) throws Exception {
        this.engine.put(key, value, unit.toMillis(ttl));
    }

    public IScanner scan(byte[] start, byte[] end) throws IOException, DBCloseException {
        return this.engine.scan(start,end);
    }

    public void delete(byte[] key) throws Exception {
        this.engine.delete(key);
    }

    public void close(){
        this.engine.close();
    }

    public void clean() throws Exception {
        this.engine.clean();
    }
}
