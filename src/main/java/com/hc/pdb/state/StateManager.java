package com.hc.pdb.state;

import com.hc.pdb.file.FileConstants;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * StateManager
 * 整个db状态的管理者，做成通用状态管理器
 * 总长度
 * @author han.congcong
 * @date 2019/6/12
 */

public class StateManager {
    private static final String STATE_FILE_NAME = "state";
    private static final String STATE_BAK_FILE_NAME = "bak";
    private String path;
    private RandomAccessFile file;
    private State state;

    public StateManager(String path) throws IOException {
        String stateFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
        try {
            file = new RandomAccessFile(stateFileName, "r");
        } catch (FileNotFoundException e) {
            File f = new File(path);
            f.createNewFile();
            file = new RandomAccessFile(stateFileName, "r");
        }
        load();
    }
    public void add(FileMeta fileMeta) throws IOException {
        state.addFileName(fileMeta);
        sync();
    }

    public void delete(String fileName) throws IOException {
        state.delete(fileName);
        sync();
    }

    private void sync() throws IOException {
        String bakFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX + STATE_BAK_FILE_NAME;
        File bakFile = new File(bakFileName);
        bakFile.createNewFile();

        try(FileOutputStream outputStream = new FileOutputStream(bakFile)) {
            outputStream.write(state.serialize());
        }
        String metaFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
        File metaFile = new File(metaFileName);
        if(metaFile.delete()){
            throw new RuntimeException();
        }

        if(!bakFile.renameTo(metaFile)){
            throw new RuntimeException();
        }
    }

    private void load() throws IOException {
        long length = file.length();
        ByteBuffer buffer = ByteBuffer.allocateDirect((int)length);
        file.getChannel().read(buffer);
        State state = new State();
        state.deSerialize(buffer);
        this.state = state;
    }


}
