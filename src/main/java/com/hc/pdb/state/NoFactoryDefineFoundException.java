package com.hc.pdb.state;

/**
 * NoFactoryDefineFoundException
 *
 * @author han.congcong
 * @date 2019/7/22
 */

public class NoFactoryDefineFoundException extends RuntimeException {

    public NoFactoryDefineFoundException() {
        super();
    }

    public NoFactoryDefineFoundException(String msg){
        super(msg);
    }
}
