package com.hc.pdb.conf;

public class ConfValueCastException extends RuntimeException {

    public ConfValueCastException() {
        super();
    }

    public ConfValueCastException(String reason) {
        super(reason);
    }
}
