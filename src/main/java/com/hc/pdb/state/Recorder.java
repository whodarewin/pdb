package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * Recorder
 * 记录器
 * @author han.congcong
 * @date 2019/7/22
 */

public class Recorder {
    private String id = UUID.randomUUID().toString();
    private String workerName;
    private LogRecorder logRecorder;
    private List<String> constructParams;
    private List<Recorder.RecordLog> recordLogs;

    public Recorder(String workerName, LogRecorder logRecorder) {
        this.workerName = workerName;
        this.logRecorder = logRecorder;
    }

    protected Recorder(String id,String workerName, List<RecordLog> log,LogRecorder logRecorder){
        this.id = id;
        this.workerName = workerName;
        this.logRecorder = logRecorder;
        this.recordLogs = log;
    }

    protected void startRecord() throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage("none");
        log.setRecordStage(RecordStage.BEGIN);
        logRecorder.append(log);
        this.recordLogs.add(log);
    }

    public void recordMsg(String state, List<String> params) throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage(state);
        log.setRecordStage(RecordStage.PROCESS);
        log.setParams(params);
        logRecorder.append(log);
        this.recordLogs.add(log);
    }

    protected void endRecord() throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage("none");
        log.setRecordStage(RecordStage.END);
        log.setParams(null);
        logRecorder.append(log);
        this.recordLogs.add(log);
    }

    public RecordLog getCurrent(){
        return recordLogs.get(recordLogs.size() - 1);
    }

    public List<String> getConstructParams() {
        return constructParams;
    }

    public void setConstructParams(List<String> constructParams) {
        this.constructParams = constructParams;
    }

    /**
     * define enum start,processing,end.
     */
    public static class RecordLog {
        private static final char SPLIT = '>';
        private static final ObjectMapper mapper = new ObjectMapper();
        private String id;
        private String workerName;
        private String processStage;
        private RecordStage recordStage;
        private List<String> params;


        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getProcessStage() {
            return processStage;
        }

        public void setProcessStage(String processStage) {
            this.processStage = processStage;
        }

        public List<String> getParams() {
            return params;
        }

        public void setParams(List<String> params) {
            this.params = params;
        }

        public String getWorkerName() {
            return workerName;
        }

        public void setWorkerName(String workerName) {
            this.workerName = workerName;
        }

        public RecordStage getRecordStage() {
            return recordStage;
        }

        public void setRecordStage(RecordStage recordStage) {
            this.recordStage = recordStage;
        }


        public String serialize() throws JsonProcessingException {
            return id + SPLIT + workerName + SPLIT + recordStage +
                    SPLIT + processStage +
                    SPLIT + mapper.writeValueAsString(params);
        }

        public static RecordLog deSerialize(String line) throws IOException {
            String[] strs = StringUtils.split(line,SPLIT);
            RecordLog log = new RecordLog();
            if(strs.length == 5){
                log.setId(strs[0]);
                log.setWorkerName(strs[1]);
                log.setRecordStage(RecordStage.valueOf(strs[2]));
                log.setProcessStage(strs[3]);
                log.setParams(mapper.readValue(strs[4],List.class));
            }else{
                throw new RuntimeException("log format error");
            }
            return log;
        }
    }

    public enum RecordStage{
        BEGIN,
        PROCESS,
        END,
    }
}
