package com.hc.pdb.wal;

import com.hc.pdb.ISerializable;
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

    public FileWalWriter(String fileName) throws IOException {
        this.fileName = fileName;
        walFile = new File(fileName);
        if(!walFile.createNewFile()) {
            throw new RuntimeException("can not create file " + fileName);
        }

        this.output = new FileOutputStream(walFile);
    }

    @Override
    public void write(ISerializable serializable) throws IOException {
        output.write(serializable.serialize());
        output.flush();
    }

    @Override
    public void close() throws IOException {
        this.output.close();
    }

    @Override
    public void delete() throws IOException {
        this.close();
        FileUtils.forceDelete(new File(fileName));
        LOGGER.info("delete wal success {}", fileName);
    }

    @Override
    public String getWalFileName() {
        return fileName;
    }
}
