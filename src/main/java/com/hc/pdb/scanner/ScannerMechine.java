package com.hc.pdb.scanner;

import com.hc.pdb.LockContext;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.hcc.HCCFile;
import com.hc.pdb.hcc.HCCManager;
import com.hc.pdb.mem.MemCache;
import com.hc.pdb.mem.MemCacheManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * scanner的创建者
 * @author han.congcong
 * @date 2019/6/22.
 */
public class ScannerMechine {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScannerMechine.class);

    private HCCManager hccManager;

    private MemCacheManager memCacheManager;

    public ScannerMechine(HCCManager hccManager,MemCacheManager memCacheManager){
        this.hccManager = hccManager;
        this.memCacheManager = memCacheManager;
    }


    /**
     * 创建scanner
     * @param startKey 开始key
     * @param endKey 结束key
     * @return
     */
    public IScanner createScanner(byte[] startKey, byte[] endKey) throws PDBIOException {
        Set<HCCFile> hccFiles;
        Set<MemCache> memCaches;
        try {
            LockContext.FLUSH_LOCK.readLock().lock();
            hccFiles = hccManager.searchHCCFile(startKey, endKey);
            memCaches = memCacheManager.searchMemCache(startKey, endKey);
        }finally {
            LockContext.FLUSH_LOCK.readLock().unlock();
        }
        Set<IScanner> scanners = new HashSet<>();
        for(HCCFile hccFile : hccFiles){
            scanners.add(new HCCScanner(hccFile.createReader(),startKey,endKey));
        }

        for(MemCache memCache : memCaches){
            scanners.add(new MemCacheScanner(memCache,startKey,endKey));
        }
        if(CollectionUtils.isEmpty(scanners)){
            return new NoneScanner();
        }
        FilterScanner scanner = new FilterScanner(scanners);
        return scanner;
    }
}
