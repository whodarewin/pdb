package com.hc.pdb.wal;

import com.hc.pdb.Cell;
import com.hc.pdb.exception.NoEnoughByteException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;

/**
 * @author han.congcong
 * @date 2019/7/27
 */
public class WalFileReader implements IWalReader{
    private String path;
    private RandomAccessFile walFile;
    private MappedByteBuffer byteBuffer;

    public WalFileReader(String path) throws IOException {
        this.path = path;
        walFile = new RandomAccessFile(path,"r");
        byteBuffer = walFile.getChannel().map(FileChannel.MapMode.READ_ONLY,0,walFile.length());
    }

    @Override
    public Iterator<Cell> read() {
        return new Iterator<Cell>() {
            Cell currentCell = null;
            @Override
            public boolean hasNext() {
                if(byteBuffer.position() == byteBuffer.limit()){
                    return false;
                }
                currentCell = Cell.toCell(byteBuffer);

                return true;
            }

            @Override
            public Cell next() {
                return currentCell;
            }
        };
    }

    @Override
    public void close() throws IOException {
        this.walFile.close();
    }
}
