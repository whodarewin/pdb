package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.HCCWriter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class HCCWriterTest {
    @Test
    public void testHccWriter() throws IOException {
        Configuration configuration = new Configuration();
        configuration.put(Constants.DB_PATH_KEY,"/Users/momo/software/pdb");

        HCCWriter hccWriter = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<Cell>();

        for(int i = 0; i < 100; i ++){
            Cell cell = new Cell((i + "").getBytes(),(i + "").getBytes(), 0l);
            cells.add(cell);
        }

        hccWriter.writeHCC(cells);
    }
}
