package com.hc.pdb.state;

import com.hc.pdb.PDBStatus;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.exception.PDBException;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.file.FileConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * StateManager
 * 整个db状态的管理者，做成通用状态管理器。
 * 这个类的增加删除接口都是幂等操作。
 * @author han.congcong
 * @date 2019/6/12
 */

public class StateManager implements PDBStatus.StatusListener {
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
        File pathFile = new File(path);
        if(!pathFile.exists() && pathFile.mkdirs()){
            throw new PDBIOException("create pdb data path failed");
        }

        String stateFileName = getStateFileName();
        String bakFileName = getStateBakFileName();

        File stateFile = new File(stateFileName);
        File bakFile = new File(bakFileName);

        // 重新整理硬盘数据
        // state file和bak file 如果同时存在，则删除bak file，
        // 因为bak file为老的state 信息，需要删除完成最终的状态变更。
        if(stateFile.exists()){
            if(bakFile.exists()) {
                FileUtils.forceDelete(bakFile);
            }
        }

        // state file 已经被删除了，bak 还没有重命名的时候，重命名。
        if((!stateFile.exists()) && bakFile.exists()){
            if(bakFile.renameTo(stateFile)){
                throw new PDBIOException("rename bak state file to state file error");
            }
        }

        try {
            file = new RandomAccessFile(stateFileName, "r");
        } catch (FileNotFoundException e) {
            LOGGER.info("no meta found,new db,create one, path {}", stateFileName);
            File f = new File(stateFileName);
            if(!f.createNewFile()){
                throw new PDBIOException("create state file error");
            }
            file = new RandomAccessFile(stateFileName, "r");
        }
    }

    /**
     * 检查state里面的file是否都存在
     */
    private void checkFileStatus() throws PDBException {
        //检查hcc
        if(CollectionUtils.isNotEmpty(this.getState().getFileMetas())) {
            Collection<File> files = FileUtils.listFiles(new File(path), new HCCFileFilter(), new NoPassFileFilter());
            Set<String> paths = new HashSet<>();
            files.forEach(file -> {
                paths.add(file.getAbsolutePath());
            });
            for (HCCFileMeta meta : this.getState().getFileMetas()) {
                if (!paths.contains(meta.getFilePath())) {
                    throw new PDBException("no file named " + meta.getFilePath());
                }
            }
        }
        //检查wal
    }

    public synchronized void add(HCCFileMeta fileMeta) throws Exception {
        LOGGER.info("add hcc file meta {}",fileMeta);
        state.getFileMetas().remove(fileMeta);
        state.getFileMetas().add(fileMeta);
        sync();
        notifyListener();
    }

    public synchronized void setCurrentWalFileMeta(WALFileMeta walFileMeta) throws PDBException {
        LOGGER.info("set current wal file meta {}", walFileMeta.getWalPath());
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

    public synchronized void deleteCompactingFile(String compactingID) throws Exception {
        LOGGER.info("delete compacting file,compacting id is {}", compactingID);
        state.getCompactingFileMeta().removeIf(compactingFile -> compactingFile.getCompactingID().equals(compactingID));
        sync();
        notifyListener();
    }

    public synchronized CompactingFile addCompactingFile(String toFilePath, HCCFileMeta compactedHccFileMeta, HCCFileMeta... hccFileMeta) throws PDBException {
        for (HCCFileMeta fileMeta : hccFileMeta) {
            LOGGER.info("add compacting file meta {}",fileMeta);
        }

        CompactingFile compactingFile = new CompactingFile();
        for (HCCFileMeta fileMeta : hccFileMeta) {
            compactingFile.getCompactingFiles().add(fileMeta);
        }
        compactingFile.setState(CompactingFile.BEGIN);
        compactingFile.setToFilePath(toFilePath);
        compactingFile.setCompactedHccFileMeta(compactedHccFileMeta);
        state.getCompactingFileMeta().remove(compactingFile);
        state.getCompactingFileMeta().add(compactingFile);
        sync();
        notifyListener();
        return compactingFile;
    }

    public synchronized void deleteFlushingWal(String walPath) throws Exception {
        LOGGER.info("delete flushing wal {}", walPath);
        state.getFlushingWals().removeIf(walFileMeta -> walFileMeta.getWalPath().equals(walPath));
        sync();
        notifyListener();
    }
    //todo: add change state method, no corver
    public synchronized void addFlushingWal(String walPath,String state,List param) throws PDBException {
        WALFileMeta walFileMeta = new WALFileMeta(walPath,state,param);
        this.state.getFlushingWals().remove(walFileMeta);
        this.state.getFlushingWals().add(walFileMeta);
        sync();
        notifyListener();
    }

    public synchronized void addFlushingWal(WALFileMeta walFileMeta) throws PDBException {
        this.state.getFlushingWals().remove(walFileMeta);
        this.state.getFlushingWals().add(walFileMeta);
        sync();
        notifyListener();
    }

    public boolean isCompactingFile(HCCFileMeta hccFileMeta){
        for (CompactingFile file : state.getCompactingFileMeta()) {
            for (HCCFileMeta fileMeta : file.getCompactingFiles()) {
                if(fileMeta.getFilePath().equals(hccFileMeta.getFilePath())){
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized void changeCompactingFileState(String compactingID,String changeState) throws PDBException {
        state.getCompactingFileMeta().forEach(compactingFile -> {
            if(compactingFile.getCompactingID().equals(compactingID)){
                compactingFile.setState(changeState);
            }
        });
        sync();
        notifyListener();
    }

    public synchronized void setCompactingFileCompactedHccFileMeta(String compactingID,HCCFileMeta hccFileMeta) throws PDBException {
        state.getCompactingFileMeta().forEach(compactingFile -> {
            if(compactingFile.getCompactingID().equals(compactingID)){
                compactingFile.setCompactedHccFileMeta(hccFileMeta);
            }
        });
        sync();
        notifyListener();
    }

    public synchronized void setClean() throws PDBException {
        this.state.setClean(true);
        sync();
        notifyListener();
    }

    public boolean isCleaning(){
        return state.isClean();
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

    public Set<CompactingFile> getCompactingFiles(){
        return state.getCompactingFileMeta();
    }

    public Collection<WALFileMeta> getFlushingWal(){
        return state.getFlushingWals();
    }

    public WALFileMeta getCurrentWALFileMeta(){
        return state.getWalFileMeta();
    }

    private void notifyListener() throws PDBException {
        for (StateChangeListener listener : listeners) {
            listener.onChange(this.state);
        }
    }

    private void sync() throws PDBIOException {
        String bakFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX + STATE_BAK_FILE_NAME;
        File bakFile = new File(bakFileName);
        if(bakFile.exists()){
            FileUtils.deleteQuietly(bakFile);
        }
        try {
            bakFile.createNewFile();

            try (FileOutputStream outputStream = new FileOutputStream(bakFile, false)) {
                byte[] bytes = state.serialize();
                outputStream.write(bytes);
                outputStream.flush();
            }
        }catch (Exception e){
            throw new PDBIOException(e);
        }
        String metaFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
        File metaFile = new File(metaFileName);
        if(!metaFile.delete()){
            throw new PDBIOException("can not delete meta file");
        }

        if(!bakFile.renameTo(metaFile)){
            throw new PDBIOException("meta bak file "+ bakFileName + " rename to file " + metaFileName + " failed");
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
        checkFileStatus();
        notifyListener();
    }

    public void addListener(StateChangeListener listener) throws Exception {
        this.listeners.add(listener);
        listener.onChange(state);
    }

     public State getState(){
         return state;
     }

     public void close() throws PDBIOException {
        try {
            this.file.close();
        }catch (IOException e){
            throw new PDBIOException(e);
        }
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

    @Override
    public void onClose() throws PDBIOException {
        close();
    }
}


class HCCFileFilter implements IOFileFilter{

    @Override
    public boolean accept(File file) {
        return true;
    }

    @Override
    public boolean accept(File dir, String name) {
        return name.endsWith(FileConstants.DATA_FILE_SUFFIX);
    }
}

class WALFileFilter implements IOFileFilter{

    @Override
    public boolean accept(File file) {
        return true;
    }

    @Override
    public boolean accept(File dir, String name) {
        return name.endsWith(FileConstants.WAL_FILE_SUFFIX);
    }
}

class NoPassFileFilter implements IOFileFilter{

    @Override
    public boolean accept(File file) {
        return false;
    }

    @Override
    public boolean accept(File dir, String name) {
        return false;
    }
}
