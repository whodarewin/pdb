package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LogRecroder
 * RecordLog的磁盘记录。
 * 包括
 * 1。查找一个RecordLog
 * 2。查找所有没有结束的RecordLog
 * 3。append一个RecordLog
 * 当一个文件中的recordLog到达一万行以后，重新启动一个新的文件做recordLog。
 * 旧的recordLog当所有的都完成以后，删除。
 * @author han.congcong
 * @date 2019/7/22
 */

public class LogRecorder {
    private static final String BAK = ".bak";
    private static final String LOG_FILE_NAME = "worker.log";
    private String path;
    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private ScheduledThreadPoolExecutor executor;
    private File currentFile;
    private PrintWriter printWriter;
    private AtomicBoolean cleaning = new AtomicBoolean(false);
    private int rollLimit;

    public LogRecorder(String path,int rollLimit) throws IOException {
        if(rollLimit < 1){
            rollLimit = 100;
        }
        this.rollLimit = rollLimit;
        String fileName = path + LOG_FILE_NAME;
        currentFile = new File(fileName);
        this.printWriter = new PrintWriter(new FileWriter(currentFile,true));
        executor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);

        executor.scheduleWithFixedDelay(new Cleaner(rollLimit),30,30, TimeUnit.MINUTES);
        this.path = path;
    }

    public void append(Recorder.RecordLog log) throws JsonProcessingException {
        try {
            lock.readLock().lock();
            String line = log.serialize();
            printWriter.println(line);
            printWriter.flush();
        }finally {
            lock.readLock().unlock();
        }
    }

    public List<Recorder.RecordLog> getAllLogNotFinished() throws IOException {
        try{
            lock.readLock().lock();
            List<Recorder.RecordLog> rets = new ArrayList<>();

            rets.addAll(getAllLogNotFinished(currentFile));
            rets.addAll(getAllLogNotFinished(new File(getBakFileName())));

            return rets;
        }finally {
            lock.readLock().unlock();
        }
    }

    private Collection<Recorder.RecordLog> getAllLogNotFinished(File file) throws IOException {
        if(!file.exists()){
            return Collections.EMPTY_SET;
        }

        List<Recorder.RecordLog> recordLogs = getAllLog(file);
        Map<String,Recorder.RecordLog> rets = new HashMap<>();
        for (Recorder.RecordLog recordLog : recordLogs) {
            if(recordLog.getRecordStage() == Recorder.RecordStage.END){
                rets.remove(recordLog.getId());
            }else{
                rets.put(recordLog.getId(),recordLog);
            }
        }
        return rets.values();
    }


    private List<Recorder.RecordLog> getAllLog(File file) throws IOException {
        if(!file.exists()){
            return Collections.EMPTY_LIST;
        }
        List<Recorder.RecordLog> recordLogs = new ArrayList<>();
        try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
            lock.readLock().lock();
            String line = null;
            while ((line = reader.readLine()) != null) {
                Recorder.RecordLog log = Recorder.RecordLog.deSerialize(line);
                recordLogs.add(log);
            }
        }finally {
            lock.readLock().unlock();
        }
        return recordLogs;
    }

    private boolean isLogFinish(List<Recorder.RecordLog> logs, File file) throws IOException {
        List<Recorder.RecordLog> lastStageLogs = getAllLog(file);
        Map<String,Recorder.RecordLog> id2RecorderLogs = new HashMap<>();
        for (Recorder.RecordLog lastStageLog : lastStageLogs) {
            if(!Recorder.RecordStage.END.equals(lastStageLog.getRecordStage())){
                id2RecorderLogs.put(lastStageLog.getId(),lastStageLog);
            }
        }
        for (Recorder.RecordLog log : logs) {
            if(id2RecorderLogs.get(log.getId()) != null){
                return false;
            }
        }
        return true;
    }

    public void close(){
        printWriter.close();
    }

    private String getCurrentFileName(){
        return path + LOG_FILE_NAME;
    }

    private String getBakFileName(){
        return path + LOG_FILE_NAME + BAK;
    }

    public class Cleaner implements Runnable{
        private int rollLimit;

        public Cleaner(int rollLimit) {
            this.rollLimit = rollLimit;
        }

        @Override
        public void run() {
            //1 检查是否有已经有做切分的日志
            try {
                if(cleaning.compareAndSet(false,true)) {
                    waitAndDelete();
                    int count = 0;
                    BufferedReader bufferedReader = new BufferedReader(new FileReader(currentFile));
                    String line = null;
                    while ((line = bufferedReader.readLine()) != null) {
                        Recorder.RecordLog log = Recorder.RecordLog.deSerialize(line);
                        if (Recorder.RecordStage.END.equals(log.getProcessStage())) {
                            count++;
                        }
                    }

                    if (count > rollLimit) {
                        try {
                            lock.writeLock().lock();
                            printWriter.close();
                            String bakFileName = path + LOG_FILE_NAME + BAK;
                            currentFile.renameTo(new File(bakFileName));
                            currentFile = new File(path + LOG_FILE_NAME + BAK);
                            printWriter = new PrintWriter(new FileWriter(currentFile));
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }
                    waitAndDelete();
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }finally {
                cleaning.set(false);
            }
        }

        private void waitAndDelete() throws IOException, InterruptedException {
            String bakFile = path + LOG_FILE_NAME + BAK;
            File file = new File(bakFile);
            if(file.exists()){
                List<Recorder.RecordLog> notFinishedValue = getAllLog(file);
                if(CollectionUtils.isNotEmpty(notFinishedValue)){
                    String filePath = path + LOG_FILE_NAME;
                    File theFile = new File(filePath);
                    while(true){
                        if(isLogFinish(notFinishedValue,theFile)){
                            FileUtils.deleteQuietly(file);
                            return;
                        }else{
                            Thread.sleep(1000);
                        }
                    }
                }
            }
        }
    }

}
