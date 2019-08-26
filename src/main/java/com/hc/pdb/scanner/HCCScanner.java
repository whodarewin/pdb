package com.hc.pdb.scanner;

import com.hc.pdb.Cell;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.hcc.HCCReader;
import com.hc.pdb.util.Bytes;

import java.io.IOException;

/**
 * hcc file的scanner
 * @author han.congcong
 */
public class HCCScanner implements IScanner {
    private HCCReader reader;
    private byte[] end;
    private Cell current;
    private boolean scanEnd;

    public HCCScanner(HCCReader reader, byte[] start, byte[] end) throws PDBIOException {
        this.reader = reader;
        this.end = end;
        reader.seek(start);
    }


    @Override
    public Cell next() {
        if(scanEnd){
            return null;
        }
        Cell cell = reader.next();
        if(end == null || Bytes.compare(cell.getKey(),end) <= 0){
            if(cell != null) {
                current = cell;
                return current;
            }
        }
        current = null;
        scanEnd = true;
        return current;
    }

    @Override
    public Cell peek() {
        return current;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}
