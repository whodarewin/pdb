package com.hc.pdb.exception;

/**
 * 数据库已经关闭但是仍然操作数据库抛出的异常
 * @author han.congcong
 * @date 2019/8/6
 */

public class DBCloseException extends PDBException {
    public DBCloseException(String msg){
        super(msg);
    }

    public DBCloseException(Exception e){
        super(e);
    }

    public DBCloseException(String msg,Exception e){
        super(msg,e);
    }
}
