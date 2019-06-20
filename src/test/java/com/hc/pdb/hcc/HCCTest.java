package com.hc.pdb.hcc;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.util.Bytes;
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
import java.util.UUID;

/**
 * hcc 读写测试，TODO：打log日志
 */
public class HCCTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCTest.class);
    private Configuration configuration;
    private String hccFileName;
    @Before
    public void setUp() throws IOException {
        configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY,HCCTest.class.getClassLoader().getResource("").getPath());
        HCCWriter writer = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<>();
        for (int i = 0; i < 100000 ; i++) {
            Cell cell = new Cell(Bytes.toBytes(i), UUID.randomUUID().toString().getBytes(),20l);
            cells.add(cell);
        }
        hccFileName = writer.writeHCC(cells);
    }

    @Test
    public void test() throws IOException {
        HCCReader reader = new HCCReader(hccFileName,new MetaReader());
        reader.seek(Bytes.toBytes(50000));
        Cell cell = reader.next();
        LOGGER.info(new String(cell.getValue()));
        Assert.assertEquals(50000,Bytes.toInt(cell.getKey()));

        int readedCellCount = 50001;
        while((cell = reader.next()) != null){
            Assert.assertEquals(readedCellCount,Bytes.toInt(cell.getKey()));
            readedCellCount ++;
        }
        LOGGER.info("readed {} ",readedCellCount);
    }

    @After
    public void after(){
        if(hccFileName != null){
            File file = new File(hccFileName);
            LOGGER.info("begin to delete file {}", hccFileName);
            file.delete();
            LOGGER.info("success deleted file {}", hccFileName);
        }
    }
}