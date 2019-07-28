package com.hc.pdb.wal;

import com.hc.pdb.Cell;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by congcong.han on 2019/7/27.
 */
public interface IWalReader {
    Iterator<Cell> read() throws IOException;
    void close() throws IOException;
}
