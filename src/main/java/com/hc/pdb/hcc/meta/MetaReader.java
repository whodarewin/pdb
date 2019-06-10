package com.hc.pdb.hcc.meta;

import com.hc.pdb.util.Bytes;

import java.io.IOException;
import java.io.RandomAccessFile;


/**
 * MetaReader
 * meta 的reader
 * @author han.congcong
 * @date 2019/6/10
 */

public class MetaReader implements IMetaReader {

    @Override
    public MetaInfo read(RandomAccessFile file) throws IOException {
        //todo:怎样让这些固定的位置信息自动记录呢,将剩余的直接抽取出来
        byte[] metaLengthBytes = new byte[4];
        file.seek(file.length() - 4);
        file.readFully(metaLengthBytes);
        int metaL = Bytes.toInt(metaLengthBytes);
        byte[] metaByte = new byte[metaL];
        file.seek(file.length() - 4 - metaL);
        file.readFully(metaByte);

        return MetaInfo.deSerialize(metaByte);
    }
}
