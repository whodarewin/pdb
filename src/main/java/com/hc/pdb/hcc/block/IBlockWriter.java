package com.hc.pdb.hcc.block;

import com.hc.pdb.Cell;
import com.hc.pdb.hcc.WriteContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public interface IBlockWriter {
    int writeBlock(List<Cell> cells, FileOutputStream outputStream, WriteContext context) throws IOException;
}
