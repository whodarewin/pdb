package com.hc.pdb.hcc;

/**
 * hcc文件的信息
 */
public class HCCInfo {
    private String path;
    private byte[] start;
    private byte[] end;


    public HCCInfo(String path, byte[] start, byte[] end) {
        this.path = path;
        this.start = start;
        this.end = end;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public byte[] getStart() {
        return start;
    }

    public void setStart(byte[] start) {
        this.start = start;
    }

    public byte[] getEnd() {
        return end;
    }

    public void setEnd(byte[] end) {
        this.end = end;
    }
}
