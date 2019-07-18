package com.hc.pdb.compactor;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.HCCFile;
import com.hc.pdb.hcc.HCCReader;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.scanner.FilterScanner;
import com.hc.pdb.scanner.HCCScanner;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.State;
import com.hc.pdb.state.StateChangeListener;
import com.hc.pdb.state.StateManager;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
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
    private Set<HCCFileMeta> compactHCCFiles = new ConcurrentSkipListSet<>();
    private int compactThreshold;
    private StateManager stateManager;

    public Compactor(Configuration configuration, StateManager stateManager){
        int compactorSize = configuration.getInt(PDBConstants.COMPACTOR_THREAD_SIZE_KEY,
                PDBConstants.COMPACTOR_THREAD_SIZE);
        compactThreshold = configuration.getInt(PDBConstants.COMPACTOR_HCCFILE_THRESHOLD_KEY,
                PDBConstants.COMPACTOR_HCCFILE_THRESHOLD);
        compactorExecutor = Executors.newFixedThreadPool(compactorSize);
        this.stateManager = stateManager;
        stateManager.addListener(this);
    }


    @Override
    public void onChange(State state) {
        Set<HCCFileMeta> metas = state.getHccFileMetas();
        Set<HCCFileMeta> noCompactMetas = metas.stream().filter(meta -> !compactHCCFiles.contains(meta))
                .collect(Collectors.toSet());
        List<HCCFileMeta> toCompact = new ArrayList<>();
        if(noCompactMetas.size() > compactThreshold){
            toCompact = noCompactMetas.stream().sorted((o1, o2) -> (int)(o1.getCreateTime() - o2.getCreateTime()))
                    .limit(2).collect(Collectors.toList());
        }

        if(CollectionUtils.isEmpty(toCompact)){
            LOGGER.info("to compact hcc file is empty");
            return;
        }
        compactorExecutor.submit(new CompactorWorker(toCompact,stateManager));
    }


    public class CompactorWorker implements Runnable{
        public List<HCCFileMeta> hccFileMetas;
        public StateManager stateManager;

        public CompactorWorker(List<HCCFileMeta> hccFileMetas, StateManager stateManager) {
            this.hccFileMetas = hccFileMetas;
            this.stateManager = stateManager;
        }

        @Override
        public void run() {
            MetaReader metaReader = new MetaReader();
            try {
                Set<IScanner> hccScanners = new HashSet<>();
                for (HCCFileMeta hccFileMeta : hccFileMetas) {
                    IScanner scanner = new HCCScanner(new HCCFile(hccFileMeta.getFileName(),metaReader).createReader(),
                            null,null);
                    hccScanners.add(scanner);
                }
                FilterScanner scanner = new FilterScanner(hccScanners);


            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

}
