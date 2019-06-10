package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.exception.DBPathNotSetException;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.block.BlockWriter;
import com.hc.pdb.hcc.meta.MetaInfo;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class HCCWriter implements IHCCWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(HCCWriter.class);

    private Configuration configuration;
    private HCCManager manager;
    private BlockWriter blockWriter;
    private String path;
    private double errorRate;

    public HCCWriter(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "configuration can not be null");
        this.configuration = configuration;
        path = configuration.get(Constants.DB_PATH_KEY);
        errorRate = configuration.getDouble(Constants.ERROR_RATE_KEY, Constants.DEFAULT_ERROR_RATE);
        this.manager = new HCCManager(configuration);
        this.blockWriter = new BlockWriter(configuration);
    }

    @Override
    public String writeHCC(List<Cell> cells) throws IOException {
        //1 创建文件

        if (path == null) {
            throw new DBPathNotSetException();
        }

        path = FileUtils.reformatDirPath(path);


        String fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
        LOGGER.info("begin to write hcc file,fileName is {}",fileName);

        File file = new File(fileName);
        while(file.exists()) {
            fileName = path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
            file = new File(fileName);
        }

        file.createNewFile();
        long createTime = System.currentTimeMillis();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            //写prefix
            LOGGER.info("first,write hcc prefix");
            fileOutputStream.write(FileConstants.HCC_WRITE_PREFIX);
            //创建bloom filter

            ByteBloomFilter filter = new ByteBloomFilter(cells.size(), errorRate, 1, 1);
            filter.allocBloom();
            WriteContext context = new WriteContext(filter);
            LOGGER.info("second,write block,index is {}", FileConstants.HCC_WRITE_PREFIX.length );
            //2 开始写block
            int blockFinishIndex = blockWriter.writeBlock(cells, fileOutputStream, context);
            LOGGER.info("block write finish,index is {}",blockFinishIndex);
            int indexStartIndex = blockFinishIndex ;
            LOGGER.info("third, write index, index is {}",indexStartIndex);
            //3 开始写index
            //todo:优化掉toByteArray的copy
            ByteArrayOutputStream indexStream = context.getIndex();

            fileOutputStream.write(indexStream.toByteArray());

            //4 开始写bloomFilter
            ByteBloomFilter bloomFilter = context.getBloom();
            LOGGER.info("index write finished at {}",blockFinishIndex + context.getIndex().size());
            int bloomStartIndex = blockFinishIndex + context.getIndex().size();
            LOGGER.info("fourth,write bloom, index is {}",bloomStartIndex);
            bloomFilter.writeBloom(fileOutputStream);

            //4 开始写meta
            byte[] startK = cells.get(0).getKey();
            byte[] endK = cells.get(cells.size() - 1).getKey();

            MetaInfo metaInfo = new MetaInfo(createTime, startK, endK, indexStartIndex, bloomStartIndex);
            LOGGER.info("fifth,write meta info {}",metaInfo);
            byte[] bytes = metaInfo.serialize();
            fileOutputStream.write(bytes);
            fileOutputStream.write(Bytes.toBytes(bytes.length));
        }

        return fileName;
    }
}
