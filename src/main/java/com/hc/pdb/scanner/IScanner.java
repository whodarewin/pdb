package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.exception.PDBException;

import java.io.Closeable;

/**
 * IScanner
 *
 * @author han.congcong
 * @date 2019/6/11
 */

public interface IScanner extends Closeable {

    /**
     * 找到下一个并移动指针位置
     * @return
     */
    Cell next() throws PDBException;

    /**
     * 返回当前位置的Cell，不移动指针位置
     * @return
     */
    Cell peek();
}
