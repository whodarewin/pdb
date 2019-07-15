package com.hc.pdb.engine;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.scanner.IScanner;
import com.hc.pdb.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

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
        for (int i = 0; i < 3000000; i++) {
            engine.put(Bytes.toBytes(i),Bytes.toBytes(i),20);
        }

        Assert.assertEquals(Bytes.toInt(engine.get(Bytes.toBytes(10))), 10);
        IScanner scanner = engine.scan(null,null);
        int i = 0;
        while(scanner.next() != null){
            Cell cell = scanner.peek();
            int value = Bytes.toInt(cell.getValue());
            if(i == 2016491){
                System.out.println(value);
            }
            Assert.assertEquals(value,i);
            i++;
        }
    }

    @After
    public void clean() throws IOException {
        engine.clean();
    }
}
