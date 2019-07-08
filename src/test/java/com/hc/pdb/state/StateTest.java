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
        state.addFileName(new HCCFileMeta("fileName1","md5"));
        state.addFileName(new HCCFileMeta("fileName2","md52"));
        ByteBuffer buffer = ByteBuffer.allocate(state.serialize().length);
        buffer.mark();
        buffer.put(state.serialize());
        buffer.reset();
        State testState = new State();
        testState.deSerialize(buffer);
        Assert.assertEquals(state.getHccFileMetas().iterator().next().getFileName(),
                testState.getHccFileMetas().iterator().next().getFileName());
    }
}
