package com.hc.pdb.compactor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hc.pdb.Cell;
import com.hc.pdb.LockContext;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.HCCFile;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.scanner.FilterScanner;
import com.hc.pdb.scanner.HCCScanner;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.state.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Compactor
 * 将两个hcc合并成一个hcc的类
 * @author han.congcong
 * @date 2019/6/11
 */

public class Compactor implements StateChangeListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
    private ExecutorService compactorExecutor;
    private Set<String> compactHCCFiles = new HashSet<>();
    private int compactThreshold;
    private StateManager stateManager;
    private HCCWriter hccWriter;
    private String path;
    private CreashWorkerManager creashWorkerManager;

    public Compactor(Configuration configuration, StateManager stateManager, HCCWriter hccWriter,
                     CreashWorkerManager creashWorkerManager){
        int compactorSize = configuration.getInt(PDBConstants.COMPACTOR_THREAD_SIZE_KEY,
                PDBConstants.COMPACTOR_THREAD_SIZE);
        compactThreshold = configuration.getInt(PDBConstants.COMPACTOR_HCCFILE_THRESHOLD_KEY,
                PDBConstants.COMPACTOR_HCCFILE_THRESHOLD);
        compactorExecutor = Executors.newFixedThreadPool(compactorSize);
        this.path = configuration.get(PDBConstants.DB_PATH_KEY);
        this.stateManager = stateManager;
        stateManager.addListener(this);
        this.hccWriter = hccWriter;
        this.creashWorkerManager = creashWorkerManager;
    }


    @Override
    public void onChange(State state) throws JsonProcessingException {
        //todo:循环检测
        synchronized (this) {
            while (true) {
                Set<HCCFileMeta> metas = state.getHccFileMetas();
                Set<HCCFileMeta> noCompactMetas = metas.stream().filter(meta -> !compactHCCFiles.contains(meta))
                        .collect(Collectors.toSet());

                if(noCompactMetas.size() > compactThreshold) {
                    LOGGER.info("choose compact file");
                    List<HCCFileMeta> toCompact = noCompactMetas.stream()
                            .sorted((o1, o2) -> (int) (o1.getCreateTime() - o2.getCreateTime()))
                            .limit(2).collect(Collectors.toList());
                    compactHCCFiles.addAll(toCompact.stream()
                            .map(fileMeta -> fileMeta.getFileName())
                            .collect(Collectors.toList()));
                    creashWorkerManager.doWork(new CompactorWorker(toCompact,stateManager),compactorExecutor);
                }else{

                    LOGGER.info("no file to compact");
                    return;

                }
            }
        }
    }


    public class CompactorWorker implements IWorkerCreashable{
        private static final String NAME = "compactor";
        private static final String PRE_RECORD = "pre_record";
        private static final String START_COMPACT = "start_compact";
        private static final String END_COMPACT = "end_compact";
        public List<HCCFileMeta> hccFileMetas;
        public StateManager stateManager;

        public CompactorWorker(List<HCCFileMeta> hccFileMetas, StateManager stateManager) {
            this.hccFileMetas = hccFileMetas;
            this.stateManager = stateManager;
        }


        public void compact(Recorder recorder) throws JsonProcessingException {
            LOGGER.info("begin to compact two file {},{}",
                    hccFileMetas.get(0).getFileName(),
                    hccFileMetas.get(1).getFileName());
            String fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
            List<String> params = new ArrayList<>();
            hccFileMetas.forEach(hccFileMeta -> {
                params.add(hccFileMeta.getFileName());
            });
            recorder.recordMsg(START_COMPACT,params);
            MetaReader metaReader = new MetaReader();
            try {
                Set<IScanner> hccScanners = new HashSet<>();
                int size = 0;
                for (HCCFileMeta hccFileMeta : hccFileMetas) {
                    IScanner scanner = new HCCScanner(new HCCFile(hccFileMeta.getFileName(),metaReader).createReader(),
                            null,null);
                    hccScanners.add(scanner);
                    size = hccFileMeta.getKvSize() + size;
                }
                FilterScanner scanner = new FilterScanner(hccScanners);

                HCCFileMeta fileMeta = hccWriter.writeHCC(new Iterator<Cell>() {
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
                }, size,fileName);
                long time = System.currentTimeMillis();

                afterCompact(fileMeta);

                LOGGER.info("compact change state transaction end {}",System.currentTimeMillis() - time);

            } catch (IOException e) {
                e.printStackTrace();
            }

        }


        private void afterCompact(HCCFileMeta fileMeta){
            try {
                //todo:缩小锁粒度
                LOGGER.info("compact change state transaction begin");
                LockContext.flushLock.writeLock().lock();
                stateManager.add(fileMeta);

                deleteFileCompacted();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                LockContext.flushLock.writeLock().unlock();
            }
        }

        private void deleteFileCompacted() {
            hccFileMetas.forEach(meta -> {
                try {
                    LOGGER.info("begin delete compacted file {}", meta.getFileName());
                    stateManager.delete(meta.getFileName());
                    FileUtils.forceDelete(new File(meta.getFileName()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public void preWork(Recorder recorder) throws JsonProcessingException {
            List<String> params = new ArrayList<>();
            hccFileMetas.forEach(hccFileMeta -> {
                params.add(hccFileMeta.getFileName());
            });
            recorder.recordMsg(PRE_RECORD,params);
        }

        @Override
        public void doWork(Recorder recorder) throws JsonProcessingException {
            compact(recorder);
            recorder.recordMsg(END_COMPACT,null);
        }

        @Override
        public void continueWork(Recorder recorder) throws Exception {
            Recorder.RecordLog recordLog = recorder.getCurrent();
            String state = recordLog.getProcessStage();
            List<String> params = recordLog.getParams();
            if(PRE_RECORD.equals(state) || START_COMPACT.equals(state)){
                if(stateManager.exist(params.get(3))){
                    deleteFileCompacted();
                    recorder.recordMsg(END_COMPACT,null);
                }else{
                    compactHCCFiles.add(params.get(0));
                    compactHCCFiles.add(params.get(1));
                    List<HCCFileMeta> metas = params.stream()
                            .limit(2)
                            .map(s -> stateManager.getHccFileMeta(s))
                            .collect(Collectors.toList());
                    creashWorkerManager.doWork(new CompactorWorker(metas,stateManager),compactorExecutor);
                }
            }
        }
    }

}
