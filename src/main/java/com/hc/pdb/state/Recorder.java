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
    private RecordLog current;
    private RecordLog constructLog;

    public Recorder(String workerName, LogRecorder logRecorder) {
        this.workerName = workerName;
        this.logRecorder = logRecorder;
    }

    protected Recorder(String id,String workerName, LogRecorder logRecorder){
        this.id = id;
        this.workerName = workerName;
        this.logRecorder = logRecorder;
    }

    protected void startRecord() throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage("none");
        log.setRecordStage(RecordStage.BEGIN);
        logRecorder.append(log);
    }

    public void recordMsg(String state, List<String> params) throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage(state);
        log.setRecordStage(RecordStage.PROCESS);
        log.setParams(params);
        current = log;
        logRecorder.append(log);
    }

    protected void endRecord() throws JsonProcessingException {
        RecordLog log = new RecordLog();
        log.setId(id);
        log.setWorkerName(workerName);
        log.setProcessStage("none");
        log.setRecordStage(RecordStage.END);
        log.setParams(null);
        logRecorder.append(log);
    }

    public RecordLog getCurrent() {
        return current;
    }

    protected void setCurrent(RecordLog current) {
        this.current = current;
    }

    public RecordLog getConstructLog() {
        return constructLog;
    }

    public void setConstructLog(RecordLog constructLog) {
        this.constructLog = constructLog;
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
        private List<String> constructParam;
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

        public List<String> getConstructParam() {
            return constructParam;
        }

        public void setConstructParam(List<String> constructParam) {
            this.constructParam = constructParam;
        }

        public String serialize() throws JsonProcessingException {
            return id + SPLIT + workerName + SPLIT + recordStage +
                    SPLIT + processStage +
                    SPLIT + mapper.writeValueAsString(constructParam)+
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
                log.setConstructParam(mapper.readValue(strs[4],List.class));
                log.setParams(mapper.readValue(strs[5],List.class));
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
