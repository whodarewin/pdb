package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.block.BlockWriter;
import com.hc.pdb.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class HCCWriter implements IHCCWriter {
    private static final byte[] HCC_WRITE_PREFIX = "hcc".getBytes();
    private Configuration configuration;
    private HCCManager manager;
    private BlockWriter blockWriter;

    public HCCWriter(Configuration configuration) {
        Preconditions.checkNotNull(configuration,"configuration can not be null");
        this.configuration = configuration;
        this.manager = new HCCManager(configuration);
        this.blockWriter = new BlockWriter(configuration);
    }

    @Override
    public void writeHCC(List<Cell> cells) throws IOException {
        //1 创建文件
        String path = configuration.get(Constants.DB_PATH_KEY);
        if(path == null){
            throw new DBPathNotSetException();
        }
        String fileName = UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
        File file = new File(fileName);
        if(!file.exists()){
            file.createNewFile();
        }

        try(FileOutputStream fileOutputStream = new FileOutputStream(file)) {

            WriteContext context = new WriteContext();
            //2 开始写block

            long blockFinishIndex = blockWriter.writeBlock(cells, fileOutputStream, context);

            //3 开始写index
            //todo:优化掉toByteArray的copy
            ByteArrayOutputStream indexStream = context.getIndex();
            long indexFinishIndex = indexStream.size() + blockFinishIndex;
            fileOutputStream.write(indexStream.toByteArray());

            //4 开始写bloomFilter
            ByteArrayOutputStream bloomStream = context.getBloom();
            long bloomFinishIndex = bloomStream.size() + indexFinishIndex;
            fileOutputStream.write(bloomStream.toByteArray());
            //4 开始写meta
            fileOutputStream.write(Bytes.toBytes(blockFinishIndex));
            fileOutputStream.write(Bytes.toBytes(indexFinishIndex));
            fileOutputStream.write(Bytes.toBytes(bloomFinishIndex));
            fileOutputStream.write(HCC_WRITE_PREFIX);
        }
    }
}
