package com.hc.pdb.file.writer;


import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

public interface IHCCWriter {

    void writeOneHCC(ByteBuffer datas, URI uri) throws IOException;

    void writeOneHCC(byte[] datas, URI uri) throws IOException;
}
