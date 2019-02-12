package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.block.BlockWriter;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;

public class HCCWriter implements IHCCWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCWriter.class);
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
    public String writeHCC(Collection<Cell> cells) throws IOException {
        //1 创建文件
        String path = configuration.get(Constants.DB_PATH_KEY);

        if(path == null){
            throw new DBPathNotSetException();
        }

        if(path.lastIndexOf('/') != path.length() - 1){
            path = path + '/';
        }

        String fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
        File file = new File(fileName);
        if(!file.exists()){
            file.createNewFile();
        }

        try(FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            double errorRate = configuration.getDouble(Constants.ERROR_RATE_KEY,Constants.DEFAULT_ERROR_RATE);
            ByteBloomFilter filter = new ByteBloomFilter(cells.size(),errorRate,1,1);
            filter.allocBloom();
            WriteContext context = new WriteContext(filter);
            //2 开始写block

            long blockFinishIndex = blockWriter.writeBlock(cells, fileOutputStream, context);

            long indexStartIndex = blockFinishIndex + 1;

            //3 开始写index
            //todo:优化掉toByteArray的copy
            ByteArrayOutputStream indexStream = context.getIndex();

            fileOutputStream.write(indexStream.toByteArray());

            //4 开始写bloomFilter
            ByteBloomFilter bloomFilter = context.getBloom();

            long bloomStartIndex = blockFinishIndex + context.getIndex().size() + 1;
            bloomFilter.writeBloom(fileOutputStream);

            //4 开始写meta
            fileOutputStream.write(Bytes.toBytes(indexStartIndex));
            LOGGER.info("write index finish index {}", indexStartIndex);

            fileOutputStream.write(Bytes.toBytes(bloomStartIndex));
            LOGGER.info("write bloom finish index {}", bloomStartIndex);

        }

        return fileName;
    }
}
