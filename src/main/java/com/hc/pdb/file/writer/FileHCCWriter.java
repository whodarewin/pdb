package com.hc.pdb.file.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;

public class FileHCCWriter implements IHCCWriter {

    public void writeOneHCC(ByteBuffer datas, URI uri) throws IOException {
        File file = new File(uri);
        if(!file.exists()){
            file.createNewFile();
        }
        try(FileOutputStream fileWriter = new FileOutputStream(file)) {
            fileWriter.getChannel().write(datas);
        }

    }

    public void writeOneHCC(byte[] datas, URI uri) throws IOException {
        File file = new File(uri);
        if(!file.exists()){
            file.createNewFile();
        }

        try(FileOutputStream fileWriter = new FileOutputStream(file)) {
            fileWriter.write(datas);
        }
    }
}
