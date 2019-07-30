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
import com.hc.pdb.state.*;
import com.hc.pdb.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private CrashWorkerManager creashWorkerManager;
    private String path;

    /**
     * todo:启动顺序，相互依赖问题
     * 1. 创建上层对象
     * 2. 整理硬盘数据
     *  1. stateManager数据
     *  2. current wal 数据
     *  3. flush 数据
     *  4. compactor数据
     * @param configuration
     * @throws Exception
     */
    public LSMEngine(Configuration configuration) throws Exception {
        Preconditions.checkNotNull(configuration);
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        LOGGER.info("create lsm db at {}",path);

        FileUtils.createDirIfNotExist(path);


        this.configuration = configuration;
        //加载状态
        this.stateManager = new StateManager(path);
        stateManager.load();

        //整个程序只有这一个写hcc的类，无状态。
        hccWriter = new HCCWriter(configuration);

        //创建memcache

        //
        MetaReader reader = new MetaReader();
        HCCManager hccManager = new HCCManager(configuration,reader);
        List<IWorkerCrashableFactory> factories = new ArrayList<>();
        factories.add(new CompactWokerCrashableFactory(stateManager));
        factories.add(new FlusherWorkerCrashableFactory());
        creashWorkerManager = new CrashWorkerManager(path,factories);
        stateManager.addListener(hccManager);
        compactor = new Compactor(configuration,stateManager, hccWriter,creashWorkerManager);
        memCacheManager = new MemCacheManager(configuration,stateManager,hccWriter,creashWorkerManager);
        scannerMechine = new ScannerMechine(hccManager,memCacheManager);

        stateManager.load();
    }



    @Override
    public void put(byte[] key, byte[] value, long ttl) throws Exception {
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
    public void delete(byte[] key) throws Exception {
        Cell cell = new Cell(key, null, Cell.NO_TTL, false);
        this.memCacheManager.addCell(cell);
    }

    @Override
    public IScanner scan(byte[] start, byte[] end) throws IOException {
        IScanner scanner = scannerMechine.createScanner(start, end);
        return scanner;
    }
}
