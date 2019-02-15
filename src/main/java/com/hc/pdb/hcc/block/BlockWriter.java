package com.hc.pdb.hcc.block;

import com.hc.pdb.Cell;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.hcc.WriteContext;
import com.hc.pdb.util.ByteBloomFilter;
import com.hc.pdb.util.Bytes;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;

public class BlockWriter implements IBlockWriter {
    private Configuration conf;

    public BlockWriter(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public long writeBlock(Collection<Cell> cells, FileOutputStream outputStream, WriteContext context) throws IOException {
        long blockSize = conf.getLong(Constants.BLOCK_SIZE_KEY,Constants.BLOCK_SIZE);
        blockSize = blockSize * 1024;

        byte[] preKey = null;
        long blockCurSize = 0;
        long index = 0;
        writeIndex(context.getIndex(), cells.iterator().next().getKey(), index);
        for (Cell cell : cells) {
            if(preKey == null){
                preKey = cell.getKey();
            }else if(Bytes.compare(preKey,cell.getKey()) >= 0){
                throw new CellWrongOrderException();
            }
            byte[] bytes = cell.toByte();

            blockCurSize = blockCurSize + bytes.length;
            index = index + bytes.length;

            outputStream.write(bytes);

            if(blockCurSize > blockSize){
                writeIndex(context.getIndex(),cell.getKey(),index);
                blockCurSize = 0;
            }
            writeBloom(context.getBloom(),cell.getKey());
        }

        return index;
    }

    private void writeBloom(ByteBloomFilter filter, byte[] key) {
        filter.add(key);
    }

    private void writeIndex(ByteArrayOutputStream indexStream, byte[] key,long index) throws IOException {
        indexStream.write(key.length);
        indexStream.write(key);
        indexStream.write(Bytes.toBytes(index));
    }
}
