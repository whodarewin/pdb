package com.hc.pdb.state;

import com.hc.pdb.file.FileConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * StateManager
 * 整个db状态的管理者，做成通用状态管理器
 * 总长度
 * @author han.congcong
 * @date 2019/6/12
 */

public class StateManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(StateManager.class);
    private static final String STATE_FILE_NAME = "state";
    private static final String STATE_BAK_FILE_NAME = "bak";
    private String path;
    private RandomAccessFile file;
    private State state;
    private List<StateChangeListener> listeners = new CopyOnWriteArrayList<>();

    public StateManager(String path) throws Exception {
        String stateFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
        this.path = path;
        try {
            file = new RandomAccessFile(stateFileName, "r");
        } catch (FileNotFoundException e) {
            LOGGER.info("no meta found,new db,create one, path {}", stateFileName);
            File f = new File(stateFileName);
            //todo:check dir.
            f.createNewFile();
            file = new RandomAccessFile(stateFileName, "r");
        }
    }
    public void add(HCCFileMeta fileMeta) throws IOException {
        state.addFileMeta(fileMeta);
        sync();
        notifyListener();
    }

    public void add(WALFileMeta walFileMeta){
        state.addWalFileMeta(walFileMeta);
    }

    public void delete(String fileName) throws IOException {
        state.delete(fileName);
        sync();
        notifyListener();
    }

    private void notifyListener() {
        listeners.forEach((listener) -> listener.onChange(this.state));
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
        if(!metaFile.delete()){
            throw new RuntimeException("can not delete meta file");
        }

        if(!bakFile.renameTo(metaFile)){
            throw new RuntimeException();
        }
    }

    public void load() throws Exception{
        long length = file.length();
        ByteBuffer buffer = ByteBuffer.allocateDirect((int)length);
        buffer.mark();
        file.getChannel().read(buffer);
        buffer.reset();
        State state = new State();
        state.deSerialize(buffer);
        this.state = state;
        notifyListener();
    }

    public void addListener(StateChangeListener listener){
        this.listeners.add(listener);
    }
}
