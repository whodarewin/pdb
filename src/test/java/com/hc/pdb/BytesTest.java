package com.hc.pdb;

import com.hc.pdb.util.Bytes;
import junit.framework.TestCase;

/**
 * Created by congcong.han on 2019/6/7.
 */
public class BytesTest extends TestCase {
    public void testLong(){
        for (long i = 0; i < 100000000; i++) {
            byte[] bytes = Bytes.toBytes(i);
            assertEquals(Bytes.toLong(bytes),i);
        }
    }
}
