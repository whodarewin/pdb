package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import com.sun.org.apache.xpath.internal.operations.String;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * pdb数据文件状态管理
 * Created by congcong.han on 2019/6/20.
 */
public class State implements ISerializable{
    private List<String> hccFileName;

    @Override
    public void deSerialize(ByteBuffer byteBuffer) {

    }

    @Override
    public byte[] serialize() throws IOException {
        return new byte[0];
    }
}
