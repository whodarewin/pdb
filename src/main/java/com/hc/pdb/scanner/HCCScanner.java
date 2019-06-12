package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.hcc.HCCReader;

import java.io.IOException;

/**
 * hcc file的scanner
 */
public class HCCScanner implements IScanner {
    private HCCReader reader;
    private byte[] end;
    private Cell current;
    private Cell next;

    public HCCScanner(HCCReader reader, byte[] start, byte[] end) throws IOException {
        this.reader = reader;
        reader.seek(start);
        next = reader.next();
    }


    @Override
    public Cell next() throws IOException {
        Cell cell = reader.next();
        if(cell == null){
            return null;
        }
        current = cell;
        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }
}
