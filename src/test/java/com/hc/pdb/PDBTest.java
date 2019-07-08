package com.hc.pdb;

import com.hc.pdb.hcc.HCCTest;
import com.hc.pdb.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * PDBTest
 *
 * @author han.congcong
 * @date 2019/7/4
 */

public class PDBTest {
    private PDB pdb;
    @Before
    public void init() throws Exception {
        String path = HCCTest.class.getClassLoader().getResource("").getPath() + "pdb";
        PDBBuilder builder = new PDBBuilder();
        builder.path(path);
        this.pdb = builder.build();
    }

    @Test
    public void testPDB() throws IOException {
        for(int i = 0; i < 1000; i ++){
            pdb.put(Bytes.toBytes(i), Bytes.toBytes(i));
        }
    }

    @After
    public void close() throws IOException {
        pdb.clean();
    }
}
