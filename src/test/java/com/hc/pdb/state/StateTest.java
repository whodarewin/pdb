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
    public void testStateSerialize() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        State state = new State();
        state.addFileMeta(new HCCFileMeta("fileName1","md5",System.currentTimeMillis(),0));
        state.addFileMeta(new HCCFileMeta("fileName2","md52",System.currentTimeMillis(),0));
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
