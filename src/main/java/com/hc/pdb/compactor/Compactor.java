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

import javax.print.attribute.standard.MediaSize;
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

public class Compactor implements StateChangeListener, IWorkerCrashableFactory {
    public static final String NAME = "compactor";
    private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
    private ExecutorService compactorExecutor;
    private int compactThreshold;
    private StateManager stateManager;
    private HCCWriter hccWriter;
    private String path;
    private CrashWorkerManager creashWorkerManager;

    public Compactor(Configuration configuration, StateManager stateManager, HCCWriter hccWriter,
                     CrashWorkerManager creashWorkerManager){
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

                    creashWorkerManager.doWork(new CompactorWorker(toCompact,stateManager),compactorExecutor);
                }else{

                    LOGGER.info("no file to compact");
                    return;

                }
            }
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public IWorkerCrashable create(Recorder.RecordLog log) {
        return null;
    }


    public class CompactorWorker implements IWorkerCrashable{
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
                    hccFileMetas.get(0).getFilePath(),
                    hccFileMetas.get(1).getFilePath());
            String fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
            List<String> params = new ArrayList<>();
            hccFileMetas.forEach(hccFileMeta -> {
                params.add(hccFileMeta.getFilePath());
            });
            recorder.recordMsg(START_COMPACT,params);
            MetaReader metaReader = new MetaReader();
            try {
                Set<IScanner> hccScanners = new HashSet<>();
                int size = 0;
                for (HCCFileMeta hccFileMeta : hccFileMetas) {
                    IScanner scanner = new HCCScanner(new HCCFile(hccFileMeta.getFilePath(),metaReader).createReader(),
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
                    LOGGER.info("begin delete compacted file {}", meta.getFilePath());
                    stateManager.delete(meta.getFilePath());
                    FileUtils.forceDelete(new File(meta.getFilePath()));
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
        public void recordConstructParam(Recorder recorder) throws IOException {
            List<String> params = new ArrayList<>();
            hccFileMetas.forEach(hccFileMeta -> {
                params.add(hccFileMeta.getFilePath());
            });
            recorder.recordMsg(PRE_RECORD,params);
            for (HCCFileMeta hccFileMeta : hccFileMetas) {
                stateManager.add(hccFileMeta);
            }
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
                    List<HCCFileMeta> metas = params.stream()
                            .limit(2)
                            .map(s -> stateManager.getHccFileMeta(s))
                            .collect(Collectors.toList());
                    for (HCCFileMeta meta : metas) {
                        stateManager.addCompactingFile(meta);
                    }
                    creashWorkerManager.doWork(new CompactorWorker(metas,stateManager),compactorExecutor);
                }
            }
        }
    }

}
