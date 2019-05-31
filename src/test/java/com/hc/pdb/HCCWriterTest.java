package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.util.Bytes;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HCCWriterTest extends TestCase {

    public void testHcc() throws IOException {
        Configuration configuration = new Configuration();
        configuration.put(Constants.DB_PATH_KEY,"/tmp/test/");
        HCCWriter writer = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<>();
        for (int i = 0; i < 10000 ; i++) {
            Cell cell = new Cell(Bytes.toBytes(i),Bytes.toBytes(i),20l);
            cells.add(cell);
        }
        writer.writeHCC(cells);
    }
}
