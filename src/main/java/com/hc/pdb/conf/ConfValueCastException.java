package com.hc.pdb.conf;

public class ConfValueCastException extends RuntimeException {

    public ConfValueCastException() {
        super();
    }

    public ConfValueCastException(String reason) {
        super(reason);
    }

    public ConfValueCastException(Exception e){
        super(e);
    }

    public ConfValueCastException(String msg,Exception e){
        super(msg,e);
    }
}
