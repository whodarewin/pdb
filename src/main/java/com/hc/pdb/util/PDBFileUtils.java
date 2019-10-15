package com.hc.pdb.util;

import com.hc.pdb.file.FileConstants;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * FileUtils
 *
 * @author han.congcong
 * @date 2019/6/10
 */

public class PDBFileUtils {
    /**
     * 对不带/的目录增加/
     * @param dirPath
     * @return
     */
    public static String reformatDirPath(String dirPath){
        if (dirPath.lastIndexOf('/') != dirPath.length() - 1) {
            dirPath = dirPath + '/';
        }
        return dirPath;
    }

    /**
     * 创建文件夹
     * @param path 路径
     */
    public static void createDirIfNotExist(String path) throws IOException {
        File file = new File(path);
        if(file.isDirectory()){
            return;
        }else{
            if(file.isFile()){
                throw new IOException(path + " is a file");
            }
            file.mkdirs();
        }
    }

    public static String createHccFileName(String path){
        return path + UUID.randomUUID().toString() + FileConstants.DATA_FILE_SUFFIX;
    }

    public static String createHccFileFlushName(String path){
        return createHccFileName(path)+ '.' + FileConstants.DATA_FILE_FLUSH_SUFFIX;
    }

    public static String createHccFileCompactName(String path){
        return createHccFileName(path) + '.' + FileConstants.DATA_FILE_COMPACT_SUFFIX;
    }

    public static String createWalFileName(String path){
        return path + UUID.randomUUID().toString() + FileConstants.WAL_FILE_SUFFIX;
    }
}
