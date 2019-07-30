package com.hc.pdb.state;

import java.util.HashMap;
import java.util.Map;

/**
 * WorkerCreashableFactoryManager
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public class WorkerCrashableFactoryManager {

    private Map<String,IWorkerCrashableFactory> name2WorkerCrashableFactory = new HashMap<>();

    public void register(String name,IWorkerCrashableFactory workerCreashableFactory){
        name2WorkerCrashableFactory.put(name, workerCreashableFactory);
    }

    public IWorkerCrashable create(String name, Recorder.RecordLog log){
        IWorkerCrashableFactory factory = name2WorkerCrashableFactory.get(name);
        if(factory == null){
            throw new NoFactoryDefineFoundException(name);
        }
        return factory.create(log);
    }


}
