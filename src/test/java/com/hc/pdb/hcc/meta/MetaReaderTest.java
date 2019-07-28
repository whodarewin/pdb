package com.hc.pdb.hcc.meta;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.HCCWriter;
import com.hc.pdb.state.HCCFileMeta;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class MetaReaderTest{
    private RandomAccessFile randomAccessFile;

    private String hccFileName;
    @Before
    public void setUp() throws IOException {

        Configuration configuration = new Configuration();
        configuration.put(PDBConstants.DB_PATH_KEY, MetaReaderTest.class.getClassLoader().getResource("").getPath());
        String path = configuration.get(PDBConstants.DB_PATH_KEY);
        HCCWriter hccWriter = new HCCWriter(configuration);
        List<Cell> cells = new ArrayList<Cell>();

        for (int i = 0; i < 100; i++) {
            Cell cell = new Cell((i + "").getBytes(), (i + "").getBytes(), 0l, false);
            cells.add(cell);
        }
        String fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
        HCCFileMeta fileMeta = hccWriter.writeHCC(cells.iterator(),cells.size(),path);
        this.hccFileName = fileMeta.getFileName();
        randomAccessFile =
                new RandomAccessFile(hccFileName, "r");
    }

    @Test
    public void testMetaReader() throws IOException {
        MetaInfo info = new MetaReader().read(randomAccessFile);
        Assert.assertEquals(info.getIndexStartIndex(), 2883);
        Assert.assertEquals(info.getBloomStartIndex(), 2902);
    }

    @After
    public void after() throws IOException {
        if(randomAccessFile != null){
            randomAccessFile.close();
        }
        if(hccFileName != null){
            File file = new File(hccFileName);
            file.delete();
        }
    }
}
