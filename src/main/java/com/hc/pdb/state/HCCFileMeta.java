package com.hc.pdb.state;

/**
 * 文件的meta信息
 * Created by congcong.han on 2019/6/22.
 */
public class HCCFileMeta {
    private String fileName;
    private String fileMD5;

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
}
