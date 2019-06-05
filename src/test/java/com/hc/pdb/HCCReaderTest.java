package com.hc.pdb;

import com.hc.pdb.hcc.HCCReader;
import com.hc.pdb.hcc.meta.MetaReader;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * HCCReaderTest
 *
 * @author han.congcong
 * @date 2019/5/31
 */

public class HCCReaderTest extends TestCase {

    public void testRead() throws IOException {
        MetaReader metaReader = new MetaReader();
        HCCReader hccReader = new HCCReader("D:/fe4bd9ff-7f95-4399-8d99-0653eb1bcb2a.hcc",metaReader);
        System.out.println(hccReader.next());
    }
}
