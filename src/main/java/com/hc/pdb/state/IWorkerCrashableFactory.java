package com.hc.pdb.state;

import java.io.IOException;

/**
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public interface IWorkerCrashableFactory {

    String getName();
    /**
     * 创建一个{@link IWorkerCrashable}
     * @return
     */
    IWorkerCrashable create(Recorder.RecordLog log) throws IOException;
}