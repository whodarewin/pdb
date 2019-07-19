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

public class DefaultWalWriter implements IWalWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultWalWriter.class);
    private String path;
    private String fileName;
    private FileOutputStream output;
    private File walFile;

    public DefaultWalWriter(String path) throws IOException {
        this.path = path;
        this.fileName = path + UUID.randomUUID().toString() + FileConstants.WAL_FILE_SUFFIX;
        walFile = new File(fileName);
        walFile.createNewFile();
        this.output = new FileOutputStream(walFile);
    }

    @Override
    public void write(ISerializable serializable) throws IOException {
        output.write(serializable.serialize());
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

    @Override
    public void markFlush() {
        String flushName = fileName + ".flush";
        walFile.renameTo(new File(flushName));
        fileName = flushName;
    }
}
