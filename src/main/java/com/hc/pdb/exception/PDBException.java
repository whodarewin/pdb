package com.hc.pdb.exception;

/**
 * PDBException
 * PDB内部异常类基础类
 * @author han.congcong
 * @date 2019/8/14
 */

public class PDBException extends Exception {

    public PDBException(){}

    public PDBException(String msg){
        super(msg);
    }

    public PDBException(String msg,Exception e){
        super(msg,e);
    }

    public PDBException(Exception e){
        super(e);
    }
}
