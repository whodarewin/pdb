package com.hc.pdb.state;

import com.hc.pdb.compactor.Compactor;

import java.util.List;

/**
 * CompactWokerCrashableFactory
 *
 * @author han.congcong
 * @date 2019/7/30
 */

public class CompactWokerCrashableFactory implements IWorkerCrashableFactory {
    private StateManager stateManager;

    public CompactWokerCrashableFactory(StateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public String getName() {
        return Compactor.NAME;
    }

    @Override
    public IWorkerCrashable create(List<Recorder.RecordLog> log) {
        return null;
    }
}
