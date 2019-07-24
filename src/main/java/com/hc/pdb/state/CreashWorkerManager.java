package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * CreashWorkerManager
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public class CreashWorkerManager {

    private ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private LogRecorder logRecorder;

    private WorkerCreashableFactoryManager workerCreashableFactoryManager;

    public CreashWorkerManager(String path) throws IOException {
        workerCreashableFactoryManager = new WorkerCreashableFactoryManager();
        logRecorder = new LogRecorder(path,1000);
        //1 load all recorder that not finished
        List<Recorder.RecordLog> logs = logRecorder.getAllLogNotFinished();
        //2 redo all recorder
        redoWork(logs);
    }

    public void register(String name,IWorkerCreashableFactory workerCreashableFactory){
        workerCreashableFactoryManager.register(name,workerCreashableFactory);
    }

    /**
     * 执行任务
     * @param worker
     * @param service
     * @return
     */
    public Future doWork(IWorkerCreashable worker, ExecutorService service){
        Recorder recorder = new Recorder(worker.getName(),logRecorder);
        return service.submit((Callable<Object>) () -> {
             recorder.startRecord();
             worker.doWork(recorder);
             recorder.endRecord();
             return true;
        });
    }

    /**
     * 宕机后继续执行任务
     * @param logs
     */
    public void redoWork(List<Recorder.RecordLog> logs){
        ExecutorService service = Executors.newFixedThreadPool(logs.size());
        List<CompletableFuture> completableFutures = new ArrayList<>();
        for(Recorder.RecordLog log : logs){
            Recorder recorder = new Recorder(log.getId(),log.getWorkerName(),logRecorder);
            String workerName = log.getWorkerName();
            IWorkerCreashable workerCreashable = workerCreashableFactoryManager.create(workerName);

            CompletableFuture.runAsync( () -> {
                try {
                    recorder.startRecord();
                    workerCreashable.continueWork(recorder);
                    recorder.endRecord();
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            },service);
        }

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .whenComplete((r,e)->{
                    if(e != null){
                        throw new RuntimeException(e);
                    }
                }).join();
    }
}
