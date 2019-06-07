package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.HCCReader;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.Bytes;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hcc 读写测试，TODO：打log日志
 */
public class HCCTest extends TestCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCTest.class);
    private Configuration configuration;
    private String hccFileName;
    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        configuration.put(Constants.DB_PATH_KEY,HCCTest.class.getClassLoader().getResource("").getPath());
        HCCWriter writer = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<>();
        for (int i = 0; i < 10000 ; i++) {
            Cell cell = new Cell(Bytes.toBytes(i),Bytes.toBytes(i),20l);
            cells.add(cell);
        }
        hccFileName = writer.writeHCC(cells);

    }

    @Test
    public void test() throws IOException {
        HCCReader reader = new HCCReader(hccFileName,new MetaReader());
        reader.seek(Bytes.toBytes(2));
        Cell cell = reader.next();
        Assert.assertEquals(2,Bytes.toInt(cell.getValue()));
    }

    @After
    public void after(){
        if(hccFileName != null){
            File file = new File(hccFileName);
            LOGGER.info("begin to delete file {}",hccFileName);
            file.delete();
        }
    }
}
