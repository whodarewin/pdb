package com.hc.pdb.hcc;

import com.hc.pdb.Cell;

import java.io.IOException;
import java.util.Collection;

public interface IHCCWriter {

    String writeHCC(Collection<Cell> cells) throws IOException;

}
