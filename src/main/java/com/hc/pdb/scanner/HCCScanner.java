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

    public HCCScanner(HCCReader reader, byte[] start, byte[] end) throws IOException {
        this.reader = reader;
        reader.seek(start);
        current = reader.next();
    }


    @Override
    public boolean hasNext() {
         return current != null;
    }

    @Override
    public Cell next() throws IOException {
        Cell tmp = current;
        current = reader.next();
        return tmp;

    }
}
