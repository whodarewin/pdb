package com.hc.pdb.state;

/**
 * WorkerCreashableFactory
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public interface IWorkerCreashableFactory {
    /**
     * 创建一个{@link IWorkerCreashable}
     * @return
     */
    IWorkerCreashable create();
}
