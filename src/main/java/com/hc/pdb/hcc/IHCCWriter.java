package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.state.HCCFileMeta;

import java.io.IOException;
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
    HCCFileMeta writeHCC(List<Cell> cells) throws IOException;

}
