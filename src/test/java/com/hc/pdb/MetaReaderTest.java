package com.hc.pdb;

import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.hcc.meta.MetaReader;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class MetaReaderTest extends TestCase {
    private RandomAccessFile randomAccessFile;

    @Before
    public void setUp() throws IOException {

        Configuration configuration = new Configuration();
        configuration.put(Constants.DB_PATH_KEY, "/Users/momo/software/pdb");

        HCCWriter hccWriter = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<Cell>();

        for (int i = 0; i < 100; i++) {
            Cell cell = new Cell((i + "").getBytes(), (i + "").getBytes(), 0l);
            cells.add(cell);
        }

        String hccFileName = hccWriter.writeHCC(cells);

        randomAccessFile =
                new RandomAccessFile(hccFileName, "r");
    }

    @Test
    public void testMetaReader() throws IOException {
        MetaInfo info = new MetaReader().read(randomAccessFile);
        assertEquals(info.getIndexStartIndex(), 1281);
        assertEquals(info.getBloomStartIndex(), 1291);
    }
}
