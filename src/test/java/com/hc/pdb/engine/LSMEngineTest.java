package com.hc.pdb.engine;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

/**
 * LSMEngineTest
 *
 * @author han.congcong
 * @date 2019/6/10
 */

public class LSMEngineTest {
    private LSMEngine engine;

    @Before
    public void init() throws Exception {
        String path = LSMEngineTest.class.getClassLoader().getResource("").getPath();
        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,path);
        engine = new LSMEngine(configuration);
    }

    @Test
    public void test() throws IOException {
        for (int i = 0; i < 10000000; i++) {
            engine.put(Bytes.toBytes(i), UUID.randomUUID().toString().getBytes(),20);
        }
    }
}
