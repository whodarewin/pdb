package com.hc.pdb.state;

import java.util.Objects;

/**
 * 文件的meta信息
 * Created by congcong.han on 2019/6/22.
 */
public class HCCFileMeta {
    private String fileName;
    private String fileMD5;
    private long createTime;
    /**
     * hcc file 重的kv数目
     */

    private long kvSize;

    public HCCFileMeta() {}

    public HCCFileMeta(String fileName, String fileMD5, long createTime, int kvSize){
        this.fileName = fileName;
        this.fileMD5 = fileMD5;
        this.createTime = createTime;
        this.kvSize = kvSize;
    }

    public String getFileName(){
        return fileName;
    }

    public String getFileMD5(){
        return fileMD5;
    }

    public long getCreateTime(){
        return createTime;
    }

    public long getKvSize(){
        return kvSize;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HCCFileMeta fileMeta = (HCCFileMeta) o;
        return createTime == fileMeta.createTime &&
                kvSize == fileMeta.kvSize &&
                Objects.equals(fileName, fileMeta.fileName) &&
                Objects.equals(fileMD5, fileMeta.fileMD5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, fileMD5, createTime, kvSize);
    }
}
