package com.hc.pdb.engine;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LSMEngineTest
 *
 * @author han.congcong
 * @date 2019/6/10
 */

public class LSMEngineTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(LSMEngineTest.class);

    public LSMEngine create(String suffix) throws Exception {
        String path = LSMEngineTest.class.getClassLoader().getResource("").getPath()+"pdb" +
                suffix + "/";
        LOGGER.info("create pdb at {}",path);
        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,path);
        return new LSMEngine(configuration);
    }

    /**
     * case1测试 无ttl写
     * 1. 写入1000000行
     * 2. 读取验证
     * @throws Exception
     */
    @Test
    public void testCase1() throws Exception {
        LSMEngine engine = create("testCase1");
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }
        for(int i = 0; i < 1000000; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            int value = Bytes.toInt(bytes);
            Assert.assertEquals(i,value);
        }
        clean(engine);
    }

    /**
     * case2测试 有ttl写 finish
     * 1. 写入1000000行 ttl 20s
     * 2. Thread.sleep(20000)
     * 3. 读取不到任何数据
     * @throws Exception
     */
    @Test
    public void testCase2() throws Exception {
        LSMEngine engine = create("testCase2");
        for(int i = 0; i < 100; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), 20);
        }
        Thread.sleep(20000);

        for(int i = 0; i < 100; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            Assert.assertEquals(null,bytes);
        }
        clean(engine);
    }

    /**
     * case3 删除验证 finish
     * 1. 写入1000000行
     * 2. 删除前1000行
     * 3. 读取验证
     * @throws Exception
     */
    @Test
    public void testCase3() throws Exception {
        LSMEngine engine = create("testCase3");
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
        clean(engine);
    }

    @Test
    public void testClean() throws Exception {
        LSMEngine engine = create("testClean");
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }

        engine.clean();

        for (int i = 0; i < 1000000; i++) {
            Assert.assertEquals(null, engine.get(Bytes.toBytes(i)));
        }
        clean(engine);
    }

    /**
     * wal稳定性
     * 1. 写入1000000个数据
     * 2. 关闭PDB
     * 3. 在这个路径上重启PDB
     * 4. 验证写入的数据。
     * @throws Exception
     */
    @Test
    public void testCase4() throws Exception {
        String path = LSMEngineTest.class.getClassLoader().getResource("").getPath()+"pdb"
                + "testCase4" + "/";
        LOGGER.info("create pdb at {}",path);
        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,path);
        LSMEngine engine = new LSMEngine(configuration);
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }
        LOGGER.info("test case 4 engine close" );
        engine.close();
        engine = new LSMEngine(configuration);
        for(int i = 0; i < 1000000; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            int value = Bytes.toInt(bytes);
            Assert.assertEquals(i,value);
        }
        engine.clean();
        LOGGER.info("test case 4 finish");
    }

    @Test
    public void testCase5()throws Exception{
        LOGGER.info("test case 5 start");
        String path = LSMEngineTest.class.getClassLoader().getResource("").getPath()+"pdb"
                + "testCase5" + "/";
        LOGGER.info("create pdb at {}",path);
        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,path);
        configuration.put(PDBConstants.COMPACTOR_HCCFILE_THRESHOLD_KEY,1);
        LSMEngine engine = new LSMEngine(configuration);
        for(int i = 0; i < 1000000; i++){
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i), Cell.NO_TTL);
        }

        engine.close();
        engine = new LSMEngine(configuration);
        for(int i = 0; i < 1000000; i++){
            byte[] bytes = engine.get(Bytes.toBytes(i));
            int value = Bytes.toInt(bytes);
            Assert.assertEquals(i,value);
        }
        engine.clean();
        LOGGER.info("test case 5 end");
    }



    public void clean(LSMEngine engine) throws Exception {
        engine.clean();
    }
}
