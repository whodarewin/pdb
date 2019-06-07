package com.hc.pdb.hcc.block;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.WriteContext;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class BlockWriter implements IBlockWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockWriter.class);
    private Configuration conf;
    private int indexShift = FileConstants.HCC_WRITE_PREFIX.length - 1;

    public BlockWriter(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public int writeBlock(List<Cell> cells, FileOutputStream outputStream, WriteContext context)
            throws IOException {
        long blockSize = conf.getLong(Constants.BLOCK_SIZE_KEY, Constants.BLOCK_SIZE);
        blockSize = blockSize * 1024;

        byte[] preKey = null;
        int blockCurSize = 0;
        int index = indexShift;
        writeIndex(context.getIndex(), cells.iterator().next().getKey(), index + 1);
        for (Cell cell : cells) {
            if (preKey == null) {
                preKey = cell.getKey();
            } else if (Bytes.compare(preKey, cell.getKey()) > 0) {
                throw new CellWrongOrderException();
            }
            byte[] bytes = cell.toBytes();

            blockCurSize = blockCurSize + bytes.length;
            index = index + bytes.length;

            //写数据
            outputStream.write(bytes);

            if (blockCurSize > blockSize) {
                writeIndex(context.getIndex(), cell.getKey(), index);
                blockCurSize = 0;
            }
            writeBloom(context.getBloom(), cell.getKey());
        }


        return index;
    }

    private void writeBloom(ByteBloomFilter filter, byte[] key) {
        filter.add(key);
    }

    private void writeIndex(ByteArrayOutputStream indexStream, byte[] key, int index) throws IOException {
        LOGGER.info("begin write key length {}",key.length);
        indexStream.write(Bytes.toBytes(key.length));
        indexStream.write(key);
        LOGGER.info("begin write index {}",index);
        indexStream.write(Bytes.toBytes(index));
    }
}
