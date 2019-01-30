package com.hc.pdb.hcc.index;

import com.hc.pdb.Cell;

import java.util.List;

public interface IIndexCreator {
    byte[] createIndex(List<Cell> cells);
}
