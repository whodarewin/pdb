package com.hc.pdb.hcc;

public class DBPathNotSetException extends RuntimeException {

    public DBPathNotSetException(){
        super();
    }

    public DBPathNotSetException(String message){
        super(message);
    }
}
