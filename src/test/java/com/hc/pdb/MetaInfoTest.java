package com.hc.pdb;

import com.hc.pdb.hcc.meta.MetaInfo;
import junit.framework.TestCase;
import org.junit.Assert;

import java.io.IOException;

/**
 * Created by congcong.han on 2019/6/1.
 */
public class MetaInfoTest extends TestCase{
    public void testMetaInfo() throws IOException {
        MetaInfo metaInfo1 = new MetaInfo("1".getBytes(),"10".getBytes(),1,2);
        byte[] metaBytes = metaInfo1.serialize();
        MetaInfo metaInfo2 = MetaInfo.deSerialize(metaBytes);
        Assert.assertEquals(new String(metaInfo1.getStartKey()),new String(metaInfo2.getStartKey()));
        Assert.assertEquals(new String(metaInfo1.getEndKey()),new String(metaInfo2.getEndKey()));
        Assert.assertEquals(metaInfo1.getBloomStartIndex(),metaInfo2.getBloomStartIndex());
        Assert.assertEquals(metaInfo1.getIndexStartIndex(),metaInfo2.getIndexStartIndex());
    }
}
