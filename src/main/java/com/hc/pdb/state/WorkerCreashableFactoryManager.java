package com.hc.pdb.state;

import java.util.HashMap;
import java.util.Map;

/**
 * WorkerCreashableFactoryManager
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public class WorkerCreashableFactoryManager {
    private Map<String,IWorkerCreashableFactory> name2WorkerCreashableFactory = new HashMap<>();

    public void register(String name,IWorkerCreashableFactory workerCreashableFactory){
        name2WorkerCreashableFactory.put(name, workerCreashableFactory);
    }

    public IWorkerCreashable create(String name){
        IWorkerCreashableFactory factory = name2WorkerCreashableFactory.get(name);
        if(factory == null){
            throw new NoFactoryDefineFoundException(name);
        }
        return factory.create();
    }


}
