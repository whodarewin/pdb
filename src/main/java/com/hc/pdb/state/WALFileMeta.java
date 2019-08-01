package com.hc.pdb.state;


import java.util.Objects;

/**
 * WALFileMeta
 *
 * @author han.congcong
 * @date 2019/7/4
 */

public class WALFileMeta {
    private String walPath;
    private boolean flushing;

    public WALFileMeta(String walPath, boolean flushing) {
        this.walPath = walPath;
        this.flushing = flushing;
    }

    public String getWalPath() {
        return walPath;
    }

    public void setWalPath(String walPath) {
        this.walPath = walPath;
    }

    public boolean getFlushing() {
        return flushing;
    }

    public void setFlushing(boolean flushing) {
        this.flushing = flushing;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WALFileMeta that = (WALFileMeta) o;
        return flushing == that.flushing &&
                Objects.equals(walPath, that.walPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(walPath, flushing);
    }
}
