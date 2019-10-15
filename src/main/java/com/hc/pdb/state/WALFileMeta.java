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
    public static final String HCC_WRITE_FINISH = "hcc_write_finish";
    public static final String HCC_CHANGE_NAME_FINISH = "hcc_change_name_finish";
    public static final String CHANGE_META_DELETE_WAL_FINISH = "change_meta_del_wal_finish";
    public static final String END_FLUSH = "end_flush";
    private String walPath;
    private String state;
    private List params;

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

    public List getParams() {
        return params;
    }

    public void setParams(List params) {
        this.params = params;
    }

    /**
     * 使用walPath作为{@link WALFileMeta}的相等属性，hashCode同此，用于
     * 可幂等性的在hashMap中添加对象。
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WALFileMeta that = (WALFileMeta) o;
        return Objects.equals(walPath, that.walPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(walPath);
    }
}
