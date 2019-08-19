package com.hc.pdb.exception;

/**
 * 在构建cell的时候没有足够的byte可以使用
 */
public class NoEnoughByteException extends PDBRuntimeException {

    public NoEnoughByteException() {
        super();
    }

    public NoEnoughByteException(String msg) {
        super(msg);
    }
}
