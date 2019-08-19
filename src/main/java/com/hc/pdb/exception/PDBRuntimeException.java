package com.hc.pdb.exception;

/**
 * PDBRuntimeException
 *
 * @author han.congcong
 * @date 2019/8/14
 */

public class PDBRuntimeException extends RuntimeException{

    public PDBRuntimeException(){
        super();
    }

    public PDBRuntimeException(String msg){
        super(msg);
    }

    public PDBRuntimeException(Exception e){
        super(e);
    }

    public PDBRuntimeException(String msg,Exception e){
        super(e);
    }
}
