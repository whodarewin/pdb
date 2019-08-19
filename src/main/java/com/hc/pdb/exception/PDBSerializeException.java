package com.hc.pdb.exception;

/**
 * SerializeException
 *
 * @author han.congcong
 * @date 2019/8/14
 */

public class PDBSerializeException extends PDBException {

    public PDBSerializeException(){}

    public PDBSerializeException(String msg){
        super(msg);
    }

    public PDBSerializeException(String msg,Exception e){
        super(msg,e);
    }

    public PDBSerializeException(Exception e){
        super(e);
    }
}
