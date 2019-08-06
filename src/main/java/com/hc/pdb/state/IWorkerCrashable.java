package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

/**
 * 宕机处理器
 * @author han.congcong
 * @date 2019/6/12
 */

public interface IWorkerCrashable {

    /**
     * 获得worker的名字
     * @return
     */
    String getName();

    /**
     * 在异步执行之前进行的动作
     * 包括重建worker所需要的参数。
     * @param recorder
     */
    void recordConstructParam(Recorder recorder) throws Exception;

    /**
     * 执行工作
     */
    void doWork(Recorder recorder) throws Exception;

    /**
     * 宕机，启动了以后继续执行工作
     */
    void continueWork(Recorder recorder) throws Exception;
}
