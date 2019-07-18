package com.hc.pdb.hcc.block;

import com.hc.pdb.Cell;
import com.hc.pdb.hcc.WriteContext;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public interface IBlockWriter {

    BlockWriterResult writeBlock(Iterator<Cell> cells, FileOutputStream outputStream, WriteContext context) throws IOException;

    class BlockWriterResult{
        private int index;
        private byte[] start;
        private byte[] end;

        public BlockWriterResult(int index, byte[] start, byte[] end) {
            this.index = index;
            this.start = start;
            this.end = end;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public byte[] getStart() {
            return start;
        }

        public void setStart(byte[] start) {
            this.start = start;
        }

        public byte[] getEnd() {
            return end;
        }

        public void setEnd(byte[] end) {
            this.end = end;
        }
    }
}
