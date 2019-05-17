package com.hc.pdb.util;

import com.hc.pdb.Cell;

public class CellUtil {

    public static long calculateCellSize(Cell cell) {

        int length = cell.getKey().length
                + cell.getValue().length
                + 64 + 32;

        return length;
    }
}
