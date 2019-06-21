package com.hc.pdb.state;

import com.hc.pdb.file.FileConstants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * StateManager
 * 整个db状态的管理者，做成通用状态管理器
 * 总长度
 * @author han.congcong
 * @date 2019/6/12
 */

public class StateManager {
    private static final String STATE_FILE_NAME = "state";
    private RandomAccessFile file;

    public StateManager(String path) throws IOException {
        String stateFileName = path + STATE_FILE_NAME + FileConstants.META_FILE_SUFFIX;
        try {
            file = new RandomAccessFile(stateFileName,"rw");
        } catch (FileNotFoundException e) {
            File f = new File(path);
            f.createNewFile();
            file = new RandomAccessFile(stateFileName,"rw");
        }
        load();
        check();
    }

    private void check() {
    }

    /**
     * 从
     */
    private void load() {

    }


}
