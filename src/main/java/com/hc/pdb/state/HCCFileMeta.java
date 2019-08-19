package com.hc.pdb.state;

import java.util.Objects;

/**
 * 文件的meta信息
 * @author han.congcong
 * @data 2019/6/22
 */
public class HCCFileMeta {
    public static final String CREATE = "create";
    public static final String BEGIN_COMPACT = "begin_compact";
    /**
     * file的path
     */
    private String filePath;
    /**
     * file md5
     */
    private String fileMD5;
    /**
     * hcc file 重的kv数目
     */
    private int kvSize;
    /**
     * 创建时间
     */
    private long createTime;


    public HCCFileMeta() {}

    public HCCFileMeta(String filePath, String fileMD5, int kvSize, long createTime) {
        this.filePath = filePath;
        this.fileMD5 = fileMD5;
        this.kvSize = kvSize;
        this.createTime = createTime;
    }

    public String getFilePath(){
        return filePath;
    }

    public String getFileMD5(){
        return fileMD5;
    }

    public long getCreateTime(){
        return createTime;
    }

    public int getKvSize(){
        return kvSize;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HCCFileMeta fileMeta = (HCCFileMeta) o;
        return createTime == fileMeta.createTime &&
                kvSize == fileMeta.kvSize &&
                Objects.equals(filePath, fileMeta.filePath) &&
                Objects.equals(fileMD5, fileMeta.fileMD5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, fileMD5, createTime, kvSize);
    }

    @Override
    public String toString() {
        return "HCCFileMeta{" +
                "filePath='" + filePath + '\'' +
                ", fileMD5='" + fileMD5 + '\'' +
                ", createTime=" + createTime +
                ", kvSize=" + kvSize +
                '}';
    }
}
