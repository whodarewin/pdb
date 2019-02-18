package com.hc.pdb.hcc.meta;

import java.io.IOException;
import java.io.RandomAccessFile;

public class MetaReader implements IMetaReader {

    @Override
    public MetaInfo read(RandomAccessFile file) throws IOException {
        //todo:怎样让这些固定的位置信息自动记录呢
        file.seek(file.length() - 8 * 2);

        int indexStartIndex = file.readInt();
        int bloomStartIndex = file.readInt();

        return new MetaInfo(indexStartIndex,bloomStartIndex);
    }
}
