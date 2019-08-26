package com.hc.pdb.wal;

import com.hc.pdb.ISerializable;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.file.FileConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * DefaultWalWriter
 * 默认的Wal的writer
 * @author han.congcong
 * @date 2019/6/11
 */

public class FileWalWriter implements IWalWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileWalWriter.class);
    private String fileName;
    private FileOutputStream output;
    private File walFile;

    public FileWalWriter(String fileName) throws PDBIOException {
        try{
            this.fileName = fileName;
            walFile = new File(fileName);
            if(!walFile.exists()){
                if (!walFile.createNewFile()) {
                    throw new PDBIOException("can not create file " + fileName);
                }
            }
            this.output = new FileOutputStream(walFile);
        }catch (Exception e){
            throw new PDBIOException(e);
        }
    }

    @Override
    public void write(ISerializable serializable) throws PDBException {
        try {
            output.write(serializable.serialize());
            output.flush();
        }catch (IOException e){
            throw new PDBIOException(e);
        }
    }

    @Override
    public void close() throws PDBIOException {
        try {
            this.output.close();
        }catch (Exception e){
            throw new PDBIOException(e);
        }
    }

    @Override
    public void delete() throws PDBIOException {
        this.close();
        try{
            FileUtils.forceDelete(new File(fileName));
            LOGGER.info("delete wal success {}", fileName);
        }catch (Exception e){
            throw new PDBIOException(e);
        }
    }

    @Override
    public String getWalFileName() {
        return fileName;
    }
}
