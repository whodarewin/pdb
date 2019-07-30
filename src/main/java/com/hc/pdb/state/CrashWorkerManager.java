package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hc.pdb.flusher.FlusherCrashable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class CrashWorkerManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrashWorkerManager.class);
    private LogRecorder logRecorder;

    private WorkerCrashableFactoryManager workerCrashableFactoryManager;

    public CrashWorkerManager(String path,List<IWorkerCrashableFactory> factories) throws IOException {
        workerCrashableFactoryManager = new WorkerCrashableFactoryManager();
        for (IWorkerCrashableFactory factory : factories) {
            workerCrashableFactoryManager.register(factory.getName(),factory);
        }
        workerCrashableFactoryManager.register(FlusherCrashable.FLUSHER,new FlusherWorkerCrashableFactory());


        logRecorder = new LogRecorder(path,1000);
        //1 load all recorder that not finished
        List<Recorder.RecordLog> logs = logRecorder.getAllLogNotFinished();
        //2 redo all recorder
        redoWork(logs);
    }

    public void register(String name,IWorkerCrashableFactory workerCreashableFactory){
        workerCrashableFactoryManager.register(name,workerCreashableFactory);
    }

    /**
     * 执行任务
     * @param worker
     * @param service
     * @return
     */
    public Future doWork(IWorkerCrashable worker, ExecutorService service) throws Exception {
        Recorder recorder = new Recorder(worker.getName(),logRecorder);
        worker.recordConstructParam(recorder);
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
        if(logs.size() == 0){
            LOGGER.info("no log to continue");
            return;
        }

        ExecutorService service = Executors.newFixedThreadPool(logs.size());
        List<CompletableFuture> completableFutures = new ArrayList<>();
        for(Recorder.RecordLog log : logs){
            Recorder recorder = new Recorder(log.getId(),log.getWorkerName(),logRecorder);
            String workerName = log.getWorkerName();
            IWorkerCrashable workerCreashable = workerCrashableFactoryManager.create(workerName,log);

            CompletableFuture.runAsync( () -> {
                try {
                    workerCreashable.continueWork(recorder);
                    recorder.endRecord();
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            },service);
        }

        CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]))
                .whenComplete((r,e)->{
                    if(e != null){
                        throw new RuntimeException(e);
                    }
                }).join();
        service.shutdown();
    }
}
