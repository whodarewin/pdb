package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;

/**
 * 创建pdb
 */
public class PDBBuilder {
    private Configuration configuration = new Configuration();

    public PDB build(){
        return new DiskPDB(configuration);
    }

    public PDBBuilder path(String path){
        configuration.put(Constants.DB_PATH_KEY,path);
        return this;
    }

    public PDBBuilder flusherThreadSize(int flushThreadSize){
        configuration.put(Constants.FLUSHER_THREAD_SIZE_KEY,flushThreadSize);
        return this;
    }
}
