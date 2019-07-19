package com.hc.pdb.scanner;

import com.hc.pdb.Cell;

import java.io.IOException;

/**
 * NoneScanner
 * 无数据scanner
 * @author han.congcong
 * @date 2019/7/16
 */

public class NoneScanner implements IScanner {
    @Override
    public Cell next() throws IOException {
        return null;
    }

    @Override
    public Cell peek() {
        return null;
    }
}