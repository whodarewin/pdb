package com.hc.pdb;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.util.PDBFileUtils;


/**
 * PDBBuilder
 * pdb 创建者
 * @author han.congcong
 * @date 2019/6/10
 */

public class PDBBuilder {

    private Configuration configuration = new Configuration();

    public PDB build() throws Exception {
        checkPath();
        return new PDB(configuration);
    }

    private void checkPath() {
        Preconditions.checkNotNull(configuration.get(PDBConstants.DB_PATH_KEY),"pdb path is empty");
    }

    public PDBBuilder path(String path) {
        path = PDBFileUtils.reformatDirPath(path);
        configuration.put(PDBConstants.DB_PATH_KEY, path);
        return this;
    }

    public PDBBuilder flusherThreadSize(int flushThreadSize) {
        configuration.put(PDBConstants.FLUSHER_THREAD_SIZE_KEY, flushThreadSize);
        return this;
    }

    public PDBBuilder memCacheSize(int size){
        configuration.put(PDBConstants.MEM_CACHE_MAX_SIZE_KEY,size);
        return this;
    }

    public PDBBuilder blockSize(int size){
        configuration.put(PDBConstants.BLOCK_SIZE_KEY, size);
        return this;
    }
}
