package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    private Map<String,IWorkerCrashableFactory> name2WorkerCrashableFactory = new HashMap<>();

    public CrashWorkerManager(String path) throws IOException {
        logRecorder = new LogRecorder(path,1000);
    }

    public void register(String name,IWorkerCrashableFactory workerCreashableFactory){
        name2WorkerCrashableFactory.put(name, workerCreashableFactory);
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

    public void redoAllWorker() throws IOException {
        //1 load all recorder that not finished
        List<Recorder.RecordLog> logs = logRecorder.getAllLogNotFinished();
        //2 redo all recorder
        redoWork(logs);
    }

    /**
     * 宕机后继续执行任务
     * @param logs
     */
    public void redoWork(List<Recorder.RecordLog> logs) throws IOException {
        if(logs.size() == 0){
            LOGGER.info("no log to continue");
            return;
        }

        ExecutorService service = Executors.newFixedThreadPool(logs.size());
        List<CompletableFuture> completableFutures = new ArrayList<>();
        for(Recorder.RecordLog log : logs){
            Recorder recorder = new Recorder(log.getId(),log.getWorkerName(),logRecorder);
            String workerName = log.getWorkerName();
            IWorkerCrashableFactory factory = name2WorkerCrashableFactory.get(workerName);
            if(factory == null){
                LOGGER.info("can not create worker {} because worker factory is null",workerName);
                throw new RuntimeException("can not create worker");
            }
            IWorkerCrashable workerCrashable = factory.create(log);
            CompletableFuture.runAsync(() -> {
                try {
                    workerCrashable.continueWork(recorder);
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
