package com.hc.pdb.engine;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.compactor.Compactor;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.HCCManager;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.mem.MemCacheManager;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.scanner.ScannerMechine;
import com.hc.pdb.state.StateManager;
import com.hc.pdb.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * LSMEngine
 * LSM数的engine
 * todo:需要事物操作日志，供断电回滚使用
 * @author han.congcong
 * @date 2019/6/3
 */

public class LSMEngine implements IEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(LSMEngine.class);

    private Configuration configuration;
    private MemCacheManager memCacheManager;
    private ScannerMechine scannerMechine;
    private StateManager stateManager;
    private HCCWriter hccWriter;
    private Compactor compactor;

    public LSMEngine(Configuration configuration) throws Exception {
        Preconditions.checkNotNull(configuration);
        LOGGER.info("create lsm db at {}",configuration.get(PDBConstants.DB_PATH_KEY));
        FileUtils.createDirIfNotExist(configuration.get(PDBConstants.DB_PATH_KEY));
        this.configuration = configuration;
        this.stateManager = new StateManager(configuration.get(PDBConstants.DB_PATH_KEY));
        hccWriter = new HCCWriter(configuration);
        memCacheManager = new MemCacheManager(configuration,stateManager,hccWriter);
        MetaReader reader = new MetaReader();
        HCCManager hccManager = new HCCManager(configuration,reader);
        scannerMechine = new ScannerMechine(hccManager,memCacheManager);
        stateManager.addListener(hccManager);
        compactor = new Compactor(configuration,stateManager, hccWriter);
        stateManager.load();
    }



    @Override
    public void put(byte[] key, byte[] value, long ttl) throws IOException {
        Cell cell = new Cell(key, value, ttl,false);
        this.memCacheManager.addCell(cell);
    }
    //todo:同步问题
    @Override
    public void clean() throws IOException {
        String path = configuration.get(PDBConstants.DB_PATH_KEY);
        LOGGER.info("clean lsm db at path {}",path);
        org.apache.commons.io.FileUtils.deleteDirectory(new File(path));
    }

    @Override
    public void close() {

    }


    @Override
    public byte[] get(byte[] key) throws IOException {
        IScanner scanner = scannerMechine.createScanner(key,key);
        if(scanner == null){
            return null;
        }
        return scanner.next().getValue();
    }

    @Override
    public void delete(byte[] key) throws IOException {
        Cell cell = new Cell(key, null, Cell.NO_TTL, false);
        this.memCacheManager.addCell(cell);
    }

    @Override
    public IScanner scan(byte[] start, byte[] end) throws IOException {
        IScanner scanner = scannerMechine.createScanner(start, end);
        return scanner;
    }
}
