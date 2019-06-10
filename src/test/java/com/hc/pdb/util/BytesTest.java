package com.hc.pdb.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by congcong.han on 2019/6/7.
 */
public class BytesTest{

    @Test(timeout = 2000)
    public void testLong(){
        for (long i = 0; i < 100000000; i++) {
            byte[] bytes = Bytes.toBytes(i);
            Assert.assertEquals(Bytes.toLong(bytes),i);
        }
    }

    @Test(timeout = 2000)
    public void testInt(){
        for (int i = 0; i < 100000000; i++) {
            byte[] bytes = Bytes.toBytes(i);
            Assert.assertEquals(Bytes.toInt(bytes),i);
        }
    }
}
