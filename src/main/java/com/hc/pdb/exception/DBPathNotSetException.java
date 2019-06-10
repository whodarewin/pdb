package com.hc.pdb.exception;

public class DBPathNotSetException extends RuntimeException {

    public DBPathNotSetException() {
        super();
    }

    public DBPathNotSetException(String message) {
        super(message);
    }
}
