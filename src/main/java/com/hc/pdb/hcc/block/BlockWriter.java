package com.hc.pdb.hcc.block;

import com.hc.pdb.Cell;
import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBSerializeException;
import com.hc.pdb.exception.PDBStopException;
import com.hc.pdb.file.FileConstants;
import com.hc.pdb.hcc.WriteContext;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * BlockWriter
 * block 的写入
 * @author han.congcong
 * @date 2019/6/10
 */

public class BlockWriter implements IBlockWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockWriter.class);
    private Configuration conf;
    private int indexShift = FileConstants.HCC_WRITE_PREFIX.length - 1;
    private PDBStatus status;

    public BlockWriter(Configuration conf, PDBStatus pdbStatus) {
        this.conf = conf;
        this.status = pdbStatus;
    }

    @Override
    public BlockWriterResult writeBlock(Iterator<Cell> cellIterator, FileOutputStream outputStream, WriteContext context)
            throws PDBIOException, PDBSerializeException, PDBStopException {
        long blockSize = conf.getLong(PDBConstants.BLOCK_SIZE_KEY, PDBConstants.DEFAULT_BLOCK_SIZE);
        blockSize = blockSize * 1024;

        byte[] preKey = null;
        int blockCurSize = 0;
        // 后面第一个可写的位置
        int index = indexShift + 1;
        byte[] start = null, end = null;

        while (cellIterator.hasNext()) {
            if(this.status.isClose()){
                LOGGER.warn("pdb is closed");
                throw new PDBStopException();
            }
            Cell cell = cellIterator.next();
            if(start == null){
                start = cell.getKey();
                writeIndex(context.getIndex(), start, index);
            }
            end = cell.getKey();
            if (preKey == null) {
                preKey = cell.getKey();
            } else if (Bytes.compare(preKey, cell.getKey()) > 0) {
                throw new CellWrongOrderException();
            }
            byte[] bytes = cell.serialize();

            blockCurSize = blockCurSize + bytes.length;
            index = index + bytes.length;

            //写数据
            try {
                outputStream.write(bytes);
            }catch (IOException e){
                throw new PDBIOException(e);
            }

            if (blockCurSize > blockSize) {
                writeIndex(context.getIndex(), cell.getKey(), index);
                blockCurSize = 0;
            }
            writeBloom(context.getBloom(), cell.getKey());
        }
        if(end == null){
            throw new RuntimeException("cell iterator is null");
        }
        //写endkey的index 形成闭环
        writeIndex(context.getIndex(), end, index);
        return new BlockWriterResult(index,start,end);
    }

    private void writeBloom(ByteBloomFilter filter, byte[] key) {
        filter.add(key);
    }

    private void writeIndex(ByteArrayOutputStream indexStream, byte[] key, int index) throws PDBIOException {
        try {
            LOGGER.debug("begin write key length {}", key.length);
            indexStream.write(Bytes.toBytes(key.length));
            indexStream.write(key);
            LOGGER.debug("begin write index {}", index);
            indexStream.write(Bytes.toBytes(index));
        }catch (Exception e){
            throw new PDBIOException(e);
        }
    }
}
