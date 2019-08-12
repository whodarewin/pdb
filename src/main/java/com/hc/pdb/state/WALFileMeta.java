package com.hc.pdb.state;


import java.util.List;
import java.util.Objects;

/**
 * WALFileMeta
 *
 * @author han.congcong
 * @date 2019/7/4
 */

public class WALFileMeta {
    public static final String CREATE = "create";
    public static final String BEGIN_FLUSH = "begin_flush";
    public static final String END_FLUSH = "end_flush";
    private String walPath;
    private String state;
    private List<String> params;

    public WALFileMeta(){}

    public WALFileMeta(String walPath, String state, List<String> params) {
        this.walPath = walPath;
        this.state = state;
        this.params = params;
    }

    public String getWalPath() {
        return walPath;
    }

    public void setWalPath(String walPath) {
        this.walPath = walPath;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WALFileMeta that = (WALFileMeta) o;
        return Objects.equals(walPath, that.walPath) &&
                Objects.equals(state, that.state) &&
                Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(walPath, state, params);
    }
}
