package com.hc.pdb.state;

import java.util.Objects;

/**
 * 文件的meta信息
 * Created by congcong.han on 2019/6/22.
 */
public class HCCFileMeta {
    /**
     * file的path
     */
    private String filePath;
    private String fileMD5;
    private long createTime;
    /**
     * hcc file 重的kv数目
     */

    private int kvSize;

    public HCCFileMeta() {}

    public HCCFileMeta(String filePath, String fileMD5, long createTime, int kvSize){
        this.filePath = filePath;
        this.fileMD5 = fileMD5;
        this.createTime = createTime;
        this.kvSize = kvSize;
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
