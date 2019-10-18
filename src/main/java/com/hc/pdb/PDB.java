package com.hc.pdb;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.engine.IEngine;
import com.hc.pdb.engine.LSMEngine;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.scanner.IScanner;

import java.util.concurrent.TimeUnit;

/**
 * PDB 对外暴露接口
 */
public class PDB {

    private IEngine engine;

    public PDB(Configuration configuration) throws PDBException {
        this.engine = new LSMEngine(configuration);
    }

    public byte[] get(byte[] key) throws PDBException {
        Preconditions.checkNotNull(key,"key can not be null");
        return engine.get(key);
    }

    public void put(byte[] key, byte[] value) throws PDBException {
        Preconditions.checkNotNull(key,"key can not be null");
        Preconditions.checkNotNull(value,"value can not be null");
        this.engine.put(key, value, -1);
    }

    public void put(byte[] key, byte[] value, long ttl, TimeUnit unit) throws PDBException {
        Preconditions.checkNotNull(key,"key can not be null");
        Preconditions.checkNotNull(value,"value can not be null");
        Preconditions.checkNotNull(unit,"unit can not be null");
        this.engine.put(key, value, unit.toMillis(ttl));
    }

    public IScanner scan(byte[] start, byte[] end) throws PDBException {
        return this.engine.scan(start,end);
    }

    public void delete(byte[] key) throws PDBException {
        Preconditions.checkNotNull(key,"key can not be null");
        this.engine.delete(key);
    }

    public void close(){
        this.engine.close();
    }

    public void clean() throws PDBException {
        this.engine.clean();
    }
}
