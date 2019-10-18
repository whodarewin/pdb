package com.hc.pdb.compactor;

import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBRuntimeException;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.HCCFile;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.scanner.FilterScanner;
import com.hc.pdb.scanner.HCCScanner;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.state.*;
import com.hc.pdb.util.NamedThreadFactory;
import com.hc.pdb.util.PDBFileUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Compactor
 * 将两个hcc合并成一个hcc的类
 * @author han.congcong
 * @date 2019/6/11
 */

public class Compactor implements StateChangeListener,IRecoveryable,
        PDBStatus.StatusListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
    public static final String NAME = "compactor";
    /**
     * compact的线程池
     */
    private ExecutorService compactorExecutor;
    /**
     * compact的阈值
     */
    private int compactThreshold;
    /**
     * 状态管理
     */
    private StateManager stateManager;
    /**
     * 全局HCCWriter
     */
    private HCCWriter hccWriter;

    private PDBStatus pdbStatus;

    private String path;

    public Compactor(Configuration configuration, StateManager stateManager,
                     HCCWriter hccWriter,PDBStatus pdbStatus) {
        int compactorSize = configuration.getInt(PDBConstants.COMPACTOR_THREAD_SIZE_KEY,
                PDBConstants.COMPACTOR_THREAD_SIZE);
        compactThreshold = configuration.getInt(PDBConstants.COMPACTOR_HCCFILE_THRESHOLD_KEY,
                PDBConstants.COMPACTOR_HCCFILE_THRESHOLD);
        if(compactThreshold < 2){
            LOGGER.info("compactor threshold can not lower than 2,which is {}", compactThreshold);
            compactThreshold = 2;
        }
        LOGGER.info("compact thread size is {}", compactorSize);
        compactorExecutor = Executors.newFixedThreadPool(compactorSize,new NamedThreadFactory("pdb-compactor"));
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        this.stateManager = stateManager;
        this.hccWriter = hccWriter;
        this.pdbStatus = pdbStatus;
    }


    @Override
    public void onChange(State state) throws PDBException {
        synchronized (this) {
            while (true) {
                Set<HCCFileMeta> allMetas = state.getFileMetas();
                Set<HCCFileMeta> noCompactMetas = allMetas.stream().filter(meta -> !stateManager.isCompactingFile(meta))
                        .collect(Collectors.toSet());

                if(noCompactMetas.size() > compactThreshold) {
                    LOGGER.info("choose compact file");
                    List<HCCFileMeta> toCompact = noCompactMetas.stream()
                            .sorted((o1, o2) -> (int) (o1.getCreateTime() - o2.getCreateTime()))
                            .limit(2).collect(Collectors.toList());
                    if(toCompact.size() < 2){
                        return;
                    }

                    String toFilePath = PDBFileUtils.createHccFileCompactName(path);
                    CompactingFile compactingFile = stateManager
                            .addCompactingFile(toFilePath,null,toCompact.toArray(new HCCFileMeta[toCompact.size()]));

                    compactorExecutor
                            .submit(new CompactorWorker(compactingFile,stateManager));
                }else{
                    LOGGER.info("no file to compact,compact threshold {} size {} compacting file size {}",
                            compactThreshold,noCompactMetas.size(),stateManager.getCompactingFiles().size());
                    return;

                }
            }
        }
    }

    @Override
    public void onClose() throws InterruptedException {
        this.compactorExecutor.shutdownNow();
        while(!this.compactorExecutor.awaitTermination(1, TimeUnit.SECONDS)){
            LOGGER.info("compactor is closing...");
        }
    }

    @Override
    public void recovery() {
        for (CompactingFile compactingFile : stateManager.getCompactingFiles()) {
            compactorExecutor.submit(new CompactorWorker(
                    compactingFile,
                    stateManager
                    ));
        }
    }


    public class CompactorWorker implements Runnable{

        private String compactingID;
        private List<HCCFileMeta> hccFileMetas;
        private StateManager stateManager;
        private String toFilePath;
        private HCCFileMeta fileMeta;
        private String state;

        public CompactorWorker(CompactingFile compactingFile, StateManager stateManager) {
            this.compactingID = compactingFile.getCompactingID();
            this.hccFileMetas = compactingFile.getCompactingFiles();
            this.stateManager = stateManager;
            this.toFilePath = compactingFile.getToFilePath();
            this.state = compactingFile.getState();
            this.fileMeta = compactingFile.getCompactedHccFileMeta();
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
                        HCCFileMeta fileMeta = writeNewHCCFile();
                        if(fileMeta != null) {
                            stateManager.changeCompactingFileState(compactingID, CompactingFile.WRITE_HCC_FILE_FINISH);
                            addNewHccFileToMeta();
                            stateManager.changeCompactingFileState(compactingID, CompactingFile.ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH);
                            deleteFileCompacted();
                            stateManager.changeCompactingFileState(compactingID, CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                            deleteMetaFileCompacting();
                        }else{
                            stateManager.changeCompactingFileState(compactingID, CompactingFile.COMPACTED_HCC_FILE_IS_NULL);
                            deleteFileCompacted();
                            stateManager.changeCompactingFileState(compactingID, CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                            deleteMetaFileCompacting();
                        }
                        break;
                    case CompactingFile.WRITE_HCC_FILE_FINISH:
                        addNewHccFileToMeta();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH);
                        deleteFileCompacted();
                        stateManager.changeCompactingFileState(compactingID,CompactingFile.DELETE_COMPACTED_FILE_FINISH);
                        deleteMetaFileCompacting();
                        break;
                    case CompactingFile.COMPACTED_HCC_FILE_IS_NULL:
                        deleteFileCompacted();
                        stateManager.changeCompactingFileState(compactingID, CompactingFile.DELETE_COMPACTED_FILE_FINISH);
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
                        throw new PDBIOException("no such state:" + state);
                }
            }catch (Exception e){
                pdbStatus.setCrashException(e);
                pdbStatus.setClosed("compact exception");
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
                LockContext.FLUSH_LOCK.writeLock().lock();
                stateManager.add(fileMeta);
                for (HCCFileMeta hccFileMeta : hccFileMetas) {
                    stateManager.delete(hccFileMeta.getFilePath());
                }
            }finally {
                LockContext.FLUSH_LOCK.writeLock().unlock();
            }
        }

        public HCCFileMeta writeNewHCCFile() throws PDBException {

            FilterScanner scanner = getScanner().getKey();
            if(scanner.next() == null){
                return null;
            }
            Pair<FilterScanner,Integer> pair = getScanner();
            FilterScanner filterScanner = pair.getKey();
            int size = pair.getValue();

            fileMeta = hccWriter.writeHCC(new Iterator<Cell>() {
                @Override
                public boolean hasNext() {
                    try {
                        return filterScanner.next() != null;
                    } catch (PDBException e) {
                        throw new PDBRuntimeException(e);
                    }
                }

                @Override
                public Cell next() {
                    return scanner.peek();
                }
            }, size, toFilePath);
            //删除和增加两个文件相互抵消
            if(fileMeta.getKvSize() == 0){
                return null;
            }
            String hccFileName = renameFile();
            fileMeta.setFilePath(hccFileName);
            stateManager.setCompactingFileCompactedHccFileMeta(compactingID,fileMeta);
            return fileMeta;
        }

        private String renameFile() throws PDBIOException {
            File file = new File(toFilePath);
            if(!file.exists()){
                throw new PDBIOException("file not found "+ toFilePath);
            }
            String hccFileName = toFilePath.substring(0,toFilePath.length() -
                    FileConstants.DATA_FILE_COMPACT_SUFFIX.length());
            if(file.renameTo(new File(hccFileName))){
                return hccFileName;
            }
            throw new PDBIOException("rename " + toFilePath + "to " + hccFileName + "error");
        }

        private void deleteFileCompacted() throws Exception {
            for (HCCFileMeta meta : hccFileMetas) {
                LOGGER.info("begin delete compacted file {}", meta.getFilePath());
                FileUtils.forceDelete(new File(meta.getFilePath()));
            }
        }

        private Pair<FilterScanner,Integer> getScanner() throws PDBIOException {
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
            return Pair.of(scanner, size);
        }
    }

}
