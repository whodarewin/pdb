package com.hc.pdb.scanner;

import com.hc.pdb.Cell;

public interface IHCCScanner {

    Cell scan(byte[] start, byte[] end);

    void seek(byte[] key);
}
