package com.hc.pdb.state;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by congcong.han on 2019/6/21.
 */
public class StateTest {
    @Test
    public void testStateSerialize() throws IOException {
        State state = new State();
        state.addFileName(new FileMeta("fileName1","md5"));
        state.addFileName(new FileMeta("fileName2","md52"));
        ByteBuffer buffer = ByteBuffer.allocate(state.serialize().length);
        buffer.mark();
        buffer.put(state.serialize());
        buffer.reset();
        State testState = new State();
        testState.deSerialize(buffer);
        Assert.assertEquals(state.getHccFileNames().iterator().next().getFileName(),
                testState.getHccFileNames().iterator().next().getFileName());
    }
}
