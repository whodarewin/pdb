package com.hc.pdb.state;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 *
 * 16:50:15.854 [hubble-client-pdb-flusher-thread--5] INFO  c.h.p.s.State - to serialize [{"filePath":"/Users/momo/sourcecode/pdb/target/test-classes/b85e59bd-b3a9-40f7-88d7-3337dec64359.hcc","fileMD5":"73aa60bbc110734af05bbe32ab43ce4a","createTime":1564649395116,"kvSize":1008247},{"filePath":"/Users/momo/sourcecode/pdb/target/test-classes/2622f38f-9670-4548-8888-3d1396d2b016.hcc","fileMD5":"a2ea1415bf1cb58b982367b17f5ba6cd","createTime":1564649376435,"kvSize":1008247},
 * {"filePath":"/Users/momo/sourcecode/pdb/target/test-classes/73c44f74-7e0a-4915-9edd-1090ed4186c4.hcc","fileMD5":"dd94b1123996e711b5a72ad4dc42d530","createTime":1564649404508,"kvSize":1008247},
 * {"filePath":"/Users/momo/sourcecode/pdb/target/test-classes/f5aedc9b-f71b-4a23-baab-45026c423cb7.hcc","fileMD5":"1f163c1bdfa250a880d81eec10d08cfd","createTime":1564649385533,"kvSize":1008247},
 * {"filePath":"/Users/momo/sourcecode/pdb/target/test-classes/c275fd4a-2a5a-4540-9a66-5e690857df50.hcc","fileMD5":"05a54b32f2af6996014608613fb16c26","createTime":1564649415079,"kvSize":1008247}]
 *  []
 *  []
 *  null
 * Created by congcong.han on 2019/6/21.
 */
public class StateTest {
    @Test
    public void testStateSerialize() throws Exception {
        State state = new State();
        state.getFileMetas().add(new HCCFileMeta("/Users/momo/sourcecode/pdb/target/test-classes/b85e59bd-b3a9-40f7-88d7-3337dec64359.hcc",
                "73aa60bbc110734af05bbe32ab43ce4a", 1008247,156495116));

        state.getFileMetas().add(new HCCFileMeta("/Users/momo/sourcecode/pdb/target/test-classes/2622f38f-9670-4548-8888-3d1396d2b016.hcc",
                "a2ea1415bf1cb58b982367b17f5ba6cd",1008247,15637643));

        state.getFileMetas().add(new HCCFileMeta("/Users/momo/sourcecode/pdb/target/test-classes/73c44f74-7e0a-4915-9edd-1090ed4186c4.hcc",
                "dd94b1123996e711b5a72ad4dc42d530",1008247,156404508l));

        state.getFileMetas().add(new HCCFileMeta("/Users/momo/sourcecode/pdb/target/test-classes/f5aedc9b-f71b-4a23-baab-45026c423cb7.hcc",
                "1f163c1bdfa250a880d81eec10d08cfd",1008247,156465533l));

        state.getFileMetas().add(new HCCFileMeta("/Users/momo/sourcecode/pdb/target/test-classes/c275fd4a-2a5a-4540-9a66-5e690857df50.hcc",
                "05a54b32f2af6996014608613fb16c26",1008247,1564649415079l));

        ByteBuffer buffer = ByteBuffer.allocate(state.serialize().length);
        buffer.mark();
        buffer.put(state.serialize());
        buffer.reset();
        State testState = new State();
        testState.deSerialize(buffer);
        Assert.assertEquals(state.getFileMetas().iterator().next().getFilePath(),
                testState.getFileMetas().iterator().next().getFilePath());

        File file = new File(StateTest.class.getClassLoader().getResource("").getPath() +  "test");
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(state.serialize());
        outputStream.flush();
        RandomAccessFile accessFile = new RandomAccessFile(StateTest.class.getClassLoader().getResource("").getPath() +  "test","r");
        long l = accessFile.length();
        ByteBuffer buffer1 = ByteBuffer.allocate((int)l);
        buffer.mark();
        accessFile.getChannel().read(buffer1);
        buffer.reset();
        state.deSerialize(buffer1);

    }
}
