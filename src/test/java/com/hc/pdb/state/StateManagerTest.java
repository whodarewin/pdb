package com.hc.pdb.state;

import com.google.common.collect.Lists;
import com.hc.pdb.exception.PDBIOException;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * StateManagerTest
 *
 * @author han.congcong
 * @date 2019/8/22
 */

public class StateManagerTest {
    private StateManager stateManager;
    private String path;

    public void before() throws Exception {
        this.path = StateManagerTest.class.getClassLoader().getResource("").getPath() + "stateManagerTestFolder/";
        stateManager = new StateManager(path);
        stateManager.load();
    }
    @Test
    public void testCase1() throws Exception {
        before();
        WALFileMeta walFileMeta = new WALFileMeta();
        walFileMeta.setState("test");
        walFileMeta.setWalPath("/testFilePath");
        walFileMeta.setParams(Lists.newArrayList("1","2"));
        stateManager.setCurrentWalFileMeta(walFileMeta);
        stateManager.close();
        before();
        WALFileMeta current = stateManager.getCurrentWALFileMeta();
        Assert.assertEquals(walFileMeta.getState(),current.getState());
        Assert.assertEquals(walFileMeta.getWalPath(),current.getWalPath());
        Assert.assertEquals(walFileMeta.getParams().get(0),current.getParams().get(0));
        Assert.assertEquals(walFileMeta.getParams().get(1),current.getParams().get(1));
        after();
    }

    @Test
    public void testCase2() throws Exception {
        before();
        WALFileMeta walFileMeta = new WALFileMeta();
        walFileMeta.setState("testCase2");
        walFileMeta.setWalPath("/testCase2FilePath");
        walFileMeta.setParams(Lists.newArrayList("testCase2"));
        stateManager.addFlushingWal(walFileMeta);
        stateManager.close();
        before();
        WALFileMeta flushing = stateManager.getFlushingWal().iterator().next();
        Assert.assertEquals(walFileMeta.getState(),flushing.getState());
        Assert.assertEquals(walFileMeta.getWalPath(),flushing.getWalPath());
        Assert.assertEquals(walFileMeta.getParams().get(0),flushing.getParams().get(0));
        after();
    }

    public void after() throws IOException, PDBIOException {
        stateManager.close();
        FileUtils.deleteDirectory(new File(path));
    }
}
