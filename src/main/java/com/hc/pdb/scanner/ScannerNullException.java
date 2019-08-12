package com.hc.pdb.scanner;

/**
 * ScannerNullException
 *
 * @author han.congcong
 * @date 2019/8/9
 */

public class ScannerNullException extends RuntimeException {
    public ScannerNullException(String msg){
        super(msg);
    }
    public ScannerNullException(String msg,Exception e){
        super(msg,e);
    }
}
