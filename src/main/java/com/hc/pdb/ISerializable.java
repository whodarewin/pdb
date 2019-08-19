package com.hc.pdb;


import com.hc.pdb.exception.PDBSerializeException;
import java.nio.ByteBuffer;

/**
 * ISerializable
 * 序列化接口
 * @author han.congcong
 * @date 2019/6/11
 */

public interface ISerializable {

    void deSerialize(ByteBuffer byteBuffer) throws PDBSerializeException;

    byte[] serialize() throws PDBSerializeException;
}
