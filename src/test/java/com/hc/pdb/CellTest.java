package com.hc.pdb;

import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by congcong.han on 2019/6/7.
 */
public class CellTest{
    @Test
    public void testCell() throws IOException {
        Cell cell1 = new Cell("1".getBytes(),"1".getBytes(),20);
        ByteBuffer buffer = ByteBuffer.allocate(cell1.serialize().length);
        buffer.mark();
        buffer.put(cell1.serialize());
        buffer.reset();
        Cell cell2 = Cell.toCell(buffer);
        Assert.assertEquals(new String(cell1.getKey()),new String(cell2.getKey()));
        Assert.assertEquals(new String(cell1.getValue()),new String(cell2.getValue()));
        Assert.assertEquals(cell1.getTtl(),cell2.getTtl());
    }
}
