package com.hc.pdb.engine;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.compactor.Compactor;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.DBCloseException;
import com.hc.pdb.hcc.HCCManager;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.mem.MemCacheManager;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.scanner.ScannerMechine;
import com.hc.pdb.state.*;
import com.hc.pdb.util.PDBFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;


/**
 * LSMEngine
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
    private String path;
    private PDBStatus pdbStatus;

    /**
     * @param configuration
     * @throws Exception
     */
    public LSMEngine(Configuration configuration) throws Exception {
        createOrStartPDB(configuration);
    }

    private void createOrStartPDB(Configuration configuration) throws Exception {
        Preconditions.checkNotNull(configuration);
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        LOGGER.info("create lsm db at {}",path);

        PDBFileUtils.createDirIfNotExist(path);

        this.pdbStatus = new PDBStatus();
        this.configuration = configuration;
        //加载状态
        this.stateManager = new StateManager(path);
        stateManager.load();

        //整个程序只有这一个写hcc的类，无状态。
        hccWriter = new HCCWriter(configuration);

        //～～ 搭建读架子
        //创建 hccManager
        MetaReader reader = new MetaReader();
        HCCManager hccManager = new HCCManager(configuration,reader);
        //注册hccManager hccFile 变动时通知
        stateManager.addListener(hccManager);
        //创建CrashWorkerManage
        memCacheManager = new MemCacheManager(configuration,stateManager,hccWriter,pdbStatus);
        //创建scannerMechine，整体的读架子搭建起来
        scannerMechine = new ScannerMechine(hccManager,memCacheManager);
        //～～ 读架子搭建完毕

        //创建compactor
        compactor = new Compactor(configuration,stateManager, hccWriter,pdbStatus);
        //注册compactor，hccFile变动时通知
        stateManager.addListener(compactor);

        pdbStatus.addListener(compactor);
        pdbStatus.addListener(memCacheManager);
    }


    @Override
    public void put(byte[] key, byte[] value, long ttl) throws Exception {
        pdbStatus.checkDBStatus();
        Cell cell = new Cell(key, value, ttl,false);
        this.memCacheManager.addCell(cell);
    }

    @Override
    public void clean() throws IOException {
        if(!pdbStatus.isClose()){
            close();
        }
        String path = configuration.get(PDBConstants.DB_PATH_KEY);
        LOGGER.info("clean lsm db at path {}",path);
        FileUtils.deleteDirectory(new File(path));
    }

    @Override
    public void close() {
        pdbStatus.setClose(true,"lsm close");
    }


    @Override
    public byte[] get(byte[] key) throws IOException, DBCloseException {
        pdbStatus.checkDBStatus();
        IScanner scanner = scannerMechine.createScanner(key,key);
        if(scanner == null){
            return null;
        }
        Cell cell = scanner.next();
        if(cell == null){
            return null;
        }
        return cell.getValue();
    }

    @Override
    public void delete(byte[] key) throws Exception {
        pdbStatus.checkDBStatus();
        Cell cell = new Cell(key, Cell.DELETE_VALUE, Cell.NO_TTL, true);
        this.memCacheManager.addCell(cell);
    }

    @Override
    public IScanner scan(byte[] start, byte[] end) throws IOException, DBCloseException {
        pdbStatus.checkDBStatus();
        IScanner scanner = scannerMechine.createScanner(start, end);
        return scanner;
    }
}
