package com.hc.pdb.state;

/**
 * RecorverFailedException
 *
 * @author han.congcong
 * @date 2019/8/10
 */

public class RecorverFailedException extends Exception {

    public RecorverFailedException(Exception e){
        super(e);
    }

    public RecorverFailedException(String msg,Exception e){
        super(msg,e);
    }
}
