package com.hc.pdb.util;

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
}
