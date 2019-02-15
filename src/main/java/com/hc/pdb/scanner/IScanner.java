package com.hc.pdb.scanner;

import com.hc.pdb.Cell;

public interface IScanner {
    Cell scan(byte[] start, byte[] end);
}
