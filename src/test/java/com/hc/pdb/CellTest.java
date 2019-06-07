package com.hc.pdb;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by congcong.han on 2019/6/7.
 */
public class CellTest extends TestCase {
    public void testCell() throws IOException {
        Cell cell1 = new Cell("1".getBytes(),"1".getBytes(),20);
        ByteBuffer buffer = ByteBuffer.allocate(cell1.toBytes().length);
        buffer.mark();
        buffer.put(cell1.toBytes());
        buffer.reset();
        Cell cell2 = Cell.toCell(buffer);
        assertEquals(new String(cell1.getKey()),new String(cell2.getKey()));
        assertEquals(new String(cell1.getValue()),new String(cell2.getValue()));
        assertEquals(cell1.getTtl(),cell2.getTtl());
    }
}
