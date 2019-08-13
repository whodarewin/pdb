package com.hc.pdb.engine;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.DBCloseException;
import com.hc.pdb.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * LSMEngineTest
 *
 * @author han.congcong
 * @date 2019/6/10
 */

public class LSMEngineTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LSMEngineTest.class);
    private LSMEngine engine;

    @Before
    public void init() throws Exception {
        String path = LSMEngineTest.class.getClassLoader().getResource("").getPath()+"pdb/";
        LOGGER.info("create pdb at {}",path);
        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,path);
        engine = new LSMEngine(configuration);
    }

    @Test
    public void testCase1() throws Exception {
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }
        for(int i = 0; i < 1000000; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            int value = Bytes.toInt(bytes);
            Assert.assertEquals(i,value);
        }
        engine.clean();
    }

    @Test
    public void testCase2() throws Exception {
        for(int i = 0; i < 100; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), 20);
        }
        Thread.sleep(20000);

        for(int i = 0; i < 100; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            Assert.assertEquals(null,bytes);
        }
        engine.clean();

    }

    @Test
    public void testCase3() throws Exception {
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }

        for(int i = 0; i < 100; i++){
            engine.delete(Bytes.toBytes(i));
        }
        for(int i = 0; i < 100; i++){
            byte[] value = engine.get(Bytes.toBytes(i));
            Assert.assertEquals(null,value);
        }
        for(int i = 100; i < 1000000; i++){
            byte[] value = engine.get(Bytes.toBytes(i));
            Assert.assertEquals(Bytes.toInt(value),i);
        }
        engine.clean();
    }

    @Test
    public void testClean() throws Exception {
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }

        engine.clean();

        testCase1();

    }

    @After
    public void clean() throws Exception {
        engine.clean();
    }
}
