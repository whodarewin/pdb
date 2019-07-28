package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

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
     * 在异步执行之前进行的动作
     * @param recorder
     */
    void preWork(Recorder recorder) throws JsonProcessingException;

    /**
     * 执行工作
     */
    void doWork(Recorder recorder) throws JsonProcessingException;

    /**
     * 宕机，启动了以后继续执行工作
     */
    void continueWork(Recorder recorder) throws IOException, Exception;
}
