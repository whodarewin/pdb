package com.hc.pdb.state;

import java.util.List;

/**
 * WALFileMeta
 *
 * @author han.congcong
 * @date 2019/7/4
 */

public class WALFileMeta {
    private String currentWal;
    private List<String> flushWal;

    public WALFileMeta(String currentWal, List<String> flushWal) {
        this.currentWal = currentWal;
        this.flushWal = flushWal;
    }

    public String getCurrentWal() {
        return currentWal;
    }

    public void setCurrentWal(String currentWal) {
        this.currentWal = currentWal;
    }

    public List<String> getFlushWal() {
        return flushWal;
    }

    public void setFlushWal(List<String> flushWal) {
        this.flushWal = flushWal;
    }
}
