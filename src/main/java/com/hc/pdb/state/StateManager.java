package com.hc.pdb.state;

import com.hc.pdb.file.FileConstants;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * StateManager
 * 整个db状态的管理者，做成通用状态管理器。
 * 这个类的增加删除接口都是幂等操作。
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

    /**
     * 初始化函数，保证数据的一致性。
     * @param path
     * @throws Exception
     */
    public StateManager(String path) throws Exception {
        this.path = path;
        String stateFileName = getStateFileName();
        String bakFileName = getStateBakFileName();
        File stateFile = new File(stateFileName);
        File bakFile = new File(bakFileName);

        //重新整理硬盘数据
        if(stateFile.exists()){
            if(bakFile.exists()) {
                FileUtils.forceDelete(bakFile);
            }
        }

        if((!stateFile.exists()) && bakFile.exists()){
            bakFile.renameTo(stateFile);
        }

        try {
            file = new RandomAccessFile(stateFileName, "r");
        } catch (FileNotFoundException e) {
            LOGGER.info("no meta found,new db,create one, path {}", stateFileName);
            File f = new File(stateFileName);
            f.createNewFile();
            file = new RandomAccessFile(stateFileName, "r");
        }
    }

    public synchronized void add(HCCFileMeta fileMeta) throws Exception {
        LOGGER.info("add hcc file meta {}",fileMeta);
        new Exception().printStackTrace();
        state.getFileMetas().add(fileMeta);
        sync();
        notifyListener();
    }

    public synchronized void setCurrentWalFileMeta(WALFileMeta walFileMeta) throws Exception {
        LOGGER.info("set current wal file meta {}", walFileMeta);
        state.setWalFileMeta(walFileMeta);
        sync();
        notifyListener();
    }

    public synchronized void delete(String filePath) throws Exception {
        LOGGER.info("delete hcc file meta {}",filePath);
        state.getFileMetas().removeIf(hccFileMeta -> hccFileMeta.getFilePath().equals(filePath));
        sync();
        notifyListener();
    }

    public synchronized void deleteCompactingFile(String filePath) throws Exception {
        LOGGER.info("delete compacting file {}", filePath);
        state.getCompactingFileMeta().removeIf(hccFileMeta -> hccFileMeta.getFilePath().equals(filePath));
        sync();
        notifyListener();
    }

    public synchronized void addCompactingFile(HCCFileMeta hccFileMeta) throws Exception {
        LOGGER.info("add compacting file meta {}",hccFileMeta);
        state.getCompactingFileMeta().add(hccFileMeta);
        sync();
        notifyListener();
    }

    public void deleteFlushingWal(String walPath) throws Exception {
        LOGGER.info("delete flushing wal {}", walPath);
        state.getFlushingWals().removeIf(walFileMeta -> walFileMeta.getWalPath().equals(walPath));
        sync();
        notifyListener();
    }

    public void addFlushingWal(String walPath){
        WALFileMeta walFileMeta = new WALFileMeta(walPath,true);
        this.state.getFlushingWals().add(walFileMeta);
    }

    public boolean isCompactingFile(HCCFileMeta hccFileMeta){
        for (HCCFileMeta fileMeta : state.getCompactingFileMeta()) {
            if(fileMeta.equals(hccFileMeta)){
                return true;
            }
        }
        return false;
    }

    public boolean exist(String fileName){
        for (HCCFileMeta hccFileMeta : state.getFileMetas()) {
            if(hccFileMeta.getFilePath().equals(fileName)){
                return true;
            }
        }
        return false;
    }

    public HCCFileMeta getHccFileMeta(String name){
        for (HCCFileMeta hccFileMeta : state.getFileMetas()) {
            if(hccFileMeta.getFilePath().equals(name)){
                return hccFileMeta;
            }
        }
        return null;
    }

    public Collection<WALFileMeta> getFlushingWal(){
        return state.getFlushingWals();
    }



    private void notifyListener() throws Exception {
        for (StateChangeListener listener : listeners) {
            listener.onChange(this.state);
        }
    }

    private void sync() throws IOException {
        String bakFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX + STATE_BAK_FILE_NAME;
        File bakFile = new File(bakFileName);
        if(bakFile.exists()){
            FileUtils.deleteQuietly(bakFile);
        }
        bakFile.createNewFile();

        try(FileOutputStream outputStream = new FileOutputStream(bakFile,false)) {
            byte[] bytes = state.serialize();
            outputStream.write(bytes);
            outputStream.flush();
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
        ByteBuffer buffer = ByteBuffer.allocate((int)length);
        buffer.mark();
        file.getChannel().read(buffer);
        file.getChannel().position(0);
        buffer.reset();
        State state = new State();
        state.deSerialize(buffer);
        this.state = state;
        notifyListener();
    }

    public void addListener(StateChangeListener listener) throws Exception {
        this.listeners.add(listener);
        listener.onChange(state);
    }

     public State getState(){
         return state;
     }

    /**
     * 获得stateFile的名字
     * @return
     */
    private String getStateFileName(){
        return path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
     }

    /**
     * 获得state file bak的名字
     * @return
     */
    private String getStateBakFileName(){
        return path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX + STATE_BAK_FILE_NAME;
     }
}
