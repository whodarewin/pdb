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
        HCCReader hccReader = new HCCReader("/tmp/test/6cc9762e-3236-4133-aa82-25d6f51e4ca0.hcc",metaReader);
        System.out.println(hccReader.next("1".getBytes()));
    }
}
