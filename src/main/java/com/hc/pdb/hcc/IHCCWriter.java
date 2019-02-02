package com.hc.pdb.hcc;

import com.hc.pdb.Cell;

import java.io.IOException;
import java.util.List;

public interface IHCCWriter {

    String writeHCC(List<Cell> cells) throws IOException;

}
