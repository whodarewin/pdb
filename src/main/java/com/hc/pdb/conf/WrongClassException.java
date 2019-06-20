package com.hc.pdb.conf;

/**
 * WrongClassException
 * {@link com.hc.pdb.conf.Configuration } 中拿到的值和期望的值不一致
 * @author han.congcong
 * @date 2019/6/3
 */

public class WrongClassException extends RuntimeException {
    public WrongClassException(){
        super();
    }

    public WrongClassException(String msg, Exception e){
        super(msg,e);
    }
}
