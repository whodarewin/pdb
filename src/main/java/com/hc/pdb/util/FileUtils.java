package com.hc.pdb.util;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;

import java.io.File;
import java.io.IOException;

/**
 * FileUtils
 *
 * @author han.congcong
 * @date 2019/6/10
 */

public class FileUtils {
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
}
