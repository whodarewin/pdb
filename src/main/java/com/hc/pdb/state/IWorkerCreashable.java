package com.hc.pdb.state;

/**
 * 宕机处理器
 * @author han.congcong
 * @date 2019/6/12
 */

public interface IWorkerCreashable {

    /**
     * 获得worker的名字
     * @return
     */
    String getName();

    /**
     * 执行工作
     */
    void doWork(Recorder recorder);

    /**
     * 宕机，启动了以后继续执行工作
     */
    void continueWork(Recorder recorder);
}
