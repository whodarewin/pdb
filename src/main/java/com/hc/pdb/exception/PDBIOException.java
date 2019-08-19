package com.hc.pdb.exception;

/**
 * PDBIOException
 * 读写文件的exception
 * @author han.congcong
 * @date 2019/8/14
 */

public class PDBIOException extends PDBException {

    public PDBIOException(){}

    public PDBIOException(String msg){
        super(msg);
    }

    public PDBIOException(Exception e){
        super(e);
    }

    public PDBIOException(String msg,Exception e){
        super(msg,e);
    }
}
