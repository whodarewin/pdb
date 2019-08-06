package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * LogRecorderTest
 *
 * @author han.congcong
 * @date 2019/7/23
 */

public class LogRecorderTest {
    private LogRecorder recorder;
    @Before
    public void setUp() throws IOException {
        String path = LogRecorderTest.class.getClassLoader().getResource("").getPath();
        recorder = new LogRecorder(path,10);
    }

    @Test
    public void testLogRecorderAppend() throws JsonProcessingException {
        Recorder.RecordLog startLog = new Recorder.RecordLog();
        startLog.setId(UUID.randomUUID().toString());
        startLog.setProcessStage("start flush");
        startLog.setRecordStage(Recorder.RecordStage.BEGIN);
        startLog.setWorkerName("test worker");
        startLog.setParams(Lists.newArrayList("a","b","c"));

        Recorder.RecordLog processLog = new Recorder.RecordLog();
        processLog.setId(UUID.randomUUID().toString());
        processLog.setProcessStage("process flush");
        processLog.setRecordStage(Recorder.RecordStage.BEGIN);
        processLog.setWorkerName("test worker");
        processLog.setParams(Lists.newArrayList("f","f","f"));

        Recorder.RecordLog endLog = new Recorder.RecordLog();
        endLog.setId(UUID.randomUUID().toString());
        endLog.setProcessStage("end flush");
        endLog.setRecordStage(Recorder.RecordStage.BEGIN);
        endLog.setWorkerName("test worker");
        endLog.setParams(Lists.newArrayList("a","b","c"));

        ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < 1000; i++) {
            service.submit(() -> {
                try {
                    recorder.append(startLog);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            });
        }
        service.shutdown();
        try {
            service.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testGetAllNoFinishedLog() throws IOException {
        List<List<Recorder.RecordLog>> logs = recorder.getAllLogNotFinished();
        Assert.assertEquals(logs.get(0).get(0).getProcessStage(),"start flush");
    }
}
