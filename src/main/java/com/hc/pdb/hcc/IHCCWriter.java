package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBSerializeException;
import com.hc.pdb.state.HCCFileMeta;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * 写一个hcc文件
 */
public interface IHCCWriter {
    /**
     * 从一个hcc文件中读取数据
     *
     * @param cells
     * @return
     * @throws IOException
     */
    HCCFileMeta writeHCC(Iterator<Cell> cells, int size, String fileName) throws PDBIOException, PDBSerializeException;

}
