package com.hc.pdb.compactor;

import com.hc.pdb.Cell;
import com.hc.pdb.ISafeClose;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.HCCFile;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.scanner.FilterScanner;
import com.hc.pdb.scanner.HCCScanner;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.state.*;
import com.hc.pdb.util.PDBFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Compactor
 * 将两个hcc合并成一个hcc的类
 * @author han.congcong
 * @date 2019/6/11
 */

public class Compactor implements StateChangeListener,IRecoveryable,
        PDBStatus.StatusListener, ISafeClose {
    public static final String NAME = "compactor";
    private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
    private ExecutorService compactorExecutor;
    private int compactThreshold;
    private StateManager stateManager;
    private HCCWriter hccWriter;
    private String path;

    public Compactor(Configuration configuration, StateManager stateManager, HCCWriter hccWriter) {
        int compactorSize = configuration.getInt(PDBConstants.COMPACTOR_THREAD_SIZE_KEY,
                PDBConstants.COMPACTOR_THREAD_SIZE);
        compactThreshold = configuration.getInt(PDBConstants.COMPACTOR_HCCFILE_THRESHOLD_KEY,
                PDBConstants.COMPACTOR_HCCFILE_THRESHOLD);
        LOGGER.info("compactor thread size is {}",compactorSize);
        compactorExecutor = Executors.newFixedThreadPool(compactorSize);
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        this.stateManager = stateManager;
        this.hccWriter = hccWriter;
    }


    @Override
    public void onChange(State state) throws Exception {
        //todo:循环检测
        synchronized (this) {
            while (true) {
                Set<HCCFileMeta> metas = state.getFileMetas();
                Set<HCCFileMeta> noCompactMetas = metas.stream().filter(meta -> !stateManager.isCompactingFile(meta))
                        .collect(Collectors.toSet());

                if(noCompactMetas.size() > compactThreshold) {
                    LOGGER.info("choose compact file");
                    List<HCCFileMeta> toCompact = noCompactMetas.stream()
                            .sorted((o1, o2) -> (int) (o1.getCreateTime() - o2.getCreateTime()))
                            .limit(2).collect(Collectors.toList());
                    if(toCompact.size() < 2){
                        return;
                    }

                    String toFilePath = PDBFileUtils.createHccFileName(path);
                    String compactingID = stateManager
                            .addCompactingFile(toFilePath,toCompact.toArray(new HCCFileMeta[toCompact.size()]));

                    compactorExecutor
                            .submit(new CompactorWorker(compactingID,toCompact,toFilePath,CompactingFile.BEGIN,stateManager));
                }else{
                    LOGGER.info("no file to compact,compact threshold {} size {} compacting file size {}",
                            compactThreshold,noCompactMetas.size(),stateManager.getCompactingFiles().size());
                    return;

                }
            }
        }
    }

    @Override
    public void onClose() {
        this.compactorExecutor.shutdown();
    }

    @Override
    public void recovery() throws RecorverFailedException {
        for (CompactingFile compactingFile : stateManager.getCompactingFiles()) {
            compactorExecutor.submit(new CompactorWorker(
                    compactingFile.getCompactingID(),
                    compactingFile.getCompactingFiles(),
                    compactingFile.getToFilePath(),
                    compactingFile.getState(),
                    stateManager
                    ));
        }
    }

    @Override
    public void safeClose() {
        PDBStatus.setClose(true);
        this.compactorExecutor.shutdownNow();

    }


    public class CompactorWorker implements Runnable{

        private String compactingID;
        private List<HCCFileMeta> hccFileMetas;
        private StateManager stateManager;
        private String toFilePath;
        private HCCFileMeta fileMeta;
        private String state;

        public CompactorWorker(String compactingID,List<HCCFileMeta> hccFileMetas,
                               String toFilePath, String state, StateManager stateManager) {
            this.compactingID = compactingID;
            this.hccFileMetas = hccFileMetas;
            this.stateManager = stateManager;
            this.toFilePath = toFilePath;
            this.state = state;
        }

        @Override
        public void run() {
            LOGGER.info("begin to compact two file {},{}",
                    hccFileMetas.get(0).getFilePath(),
                    hccFileMetas.get(1).getFilePath());
            try {
                switch (state){
                    case CompactingFile.BEGIN:
                        deleteCompactTargetFileIfHave();
                        writeNewHCCFile();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.WRITE_HCC_FILE_FINISH);
                        addNewHccFileToMeta();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH);
                        deleteFileCompacted();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                        deleteMetaFileCompacting();
                        break;
                    case CompactingFile.WRITE_HCC_FILE_FINISH:
                        addNewHccFileToMeta();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH);
                        deleteFileCompacted();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                        deleteMetaFileCompacting();
                        break;
                    case CompactingFile.ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH:
                        deleteFileCompacted();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                        deleteMetaFileCompacting();
                        break;
                    case CompactingFile.DELETE_COMPACTED_FILE_FINISH:
                        deleteMetaFileCompacting();
                        break;
                    default:
                        throw new RuntimeException("no such state:" + state);
                }
            }catch (Exception e){
                PDBStatus.setCrashException(e);
                PDBStatus.setClose(true);
                throw new RuntimeException(e);
            }
        }

        private void deleteCompactTargetFileIfHave() throws IOException {
            File file = new File(toFilePath);
            if(file.exists()){
                FileUtils.forceDelete(file);
            }
        }

        private void deleteMetaFileCompacting() throws Exception {
            stateManager.deleteCompactingFile(compactingID);
        }

        private void addNewHccFileToMeta() throws Exception {
            try {
                LockContext.flushLock.writeLock().lock();
                stateManager.add(fileMeta);
                for (HCCFileMeta hccFileMeta : hccFileMetas) {
                    stateManager.delete(hccFileMeta.getFilePath());
                }
            }finally {
                LockContext.flushLock.writeLock().unlock();
            }
        }

        public void writeNewHCCFile() throws IOException {
            MetaReader metaReader = new MetaReader();

            Set<IScanner> hccScanners = new HashSet<>();
            int size = 0;
            for (HCCFileMeta hccFileMeta : hccFileMetas) {
                IScanner scanner = new HCCScanner(new HCCFile(hccFileMeta.getFilePath(), metaReader).createReader(),
                        null, null);
                hccScanners.add(scanner);
                size = hccFileMeta.getKvSize() + size;
            }
            FilterScanner scanner = new FilterScanner(hccScanners);

            fileMeta = hccWriter.writeHCC(new Iterator<Cell>() {
                @Override
                public boolean hasNext() {
                    try {
                        return scanner.next() != null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public Cell next() {
                    return scanner.peek();
                }
            }, size, toFilePath);
        }


        private void deleteFileCompacted() throws Exception {
            for (HCCFileMeta meta : hccFileMetas) {
                LOGGER.info("begin delete compacted file {}", meta.getFilePath());
                FileUtils.forceDelete(new File(meta.getFilePath()));
            }
        }
    }

}
