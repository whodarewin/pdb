package com.hc.pdb.state;

import com.hc.pdb.flusher.FlusherCrashable;

/**
 * FlusherWorkerCreashableFactory
 *
 * @author han.congcong
 * @date 2019/7/29
 */

public class FlusherWorkerCrashableFactory implements IWorkerCrashableFactory {
    @Override
    public String getName() {
        return FlusherCrashable.FLUSHER;
    }

    @Override
    public IWorkerCrashable create(Recorder.RecordLog recordLog) {
        FlusherCrashable flusherCrashable = new FlusherCrashable();
        return flusherCrashable;
    }
}
