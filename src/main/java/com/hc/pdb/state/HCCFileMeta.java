package com.hc.pdb.state;

import java.util.Objects;

/**
 * 文件的meta信息
 * Created by congcong.han on 2019/6/22.
 */
public class HCCFileMeta {
    private String fileName;
    private String fileMD5;

    public HCCFileMeta() {}

    public HCCFileMeta(String fileName, String fileMD5){
        this.fileName = fileName;
        this.fileMD5 = fileMD5;
    }

    public String getFileName(){
        return fileName;
    }

    public String getFileMD5(){
        return fileMD5;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HCCFileMeta fileMeta = (HCCFileMeta) o;
        return Objects.equals(fileName, fileMeta.fileName) &&
                Objects.equals(fileMD5, fileMeta.fileMD5);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, fileMD5);
    }
}
