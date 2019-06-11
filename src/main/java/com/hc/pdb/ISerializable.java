package com.hc.pdb;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * ISerializable
 * 序列化接口
 * @author han.congcong
 * @date 2019/6/11
 */

public interface ISerializable {

    void deSerialize(ByteBuffer byteBuffer);

    byte[] serialize() throws IOException;
}
