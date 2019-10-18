package com.hc.pdb;

import com.hc.pdb.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
        String path = PDBTest.class.getClassLoader().getResource("").getPath() + "pdb";
        PDBBuilder builder = new PDBBuilder();
        builder.path(path);
        this.pdb = builder.build();
    }

    @Test
    public void testPDB() throws Exception {
        for(int i = 0; i < 1000; i ++){
            pdb.put(Bytes.toBytes(i), Bytes.toBytes(i));
        }
        pdb.get(Bytes.toBytes(1));
        pdb.scan(Bytes.toBytes(1),Bytes.toBytes(100));
        pdb.delete(Bytes.toBytes(1));
    }

    @After
    public void close() throws Exception {
        pdb.clean();
    }
}
