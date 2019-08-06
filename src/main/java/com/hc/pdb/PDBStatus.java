package com.hc.pdb;

import com.hc.pdb.exception.DBCloseException;

import java.util.List;

/**
 * DBStatus
 *
 * @author han.congcong
 * @date 2019/8/6
 */

public class PDBStatus {
    private static boolean close;
    private static Exception crashException;
    private static List<StatusListener> listeners;

    public static void checkDBStatus() throws DBCloseException {
        if(!close){
            throw new DBCloseException(crashException);
        }
    }

    public static boolean isClose() {
        return close;
    }

    public static void setClose(boolean close) {
        PDBStatus.close = close;
        for (StatusListener listener : listeners) {
            listener.onClose();
        }
    }

    public static Exception getCrashException() {
        return crashException;
    }

    public static void setCrashException(Exception crashException) {
        PDBStatus.crashException = crashException;
    }

    public static void addListener(StatusListener listener){
        listeners.add(listener);
    }

    public static interface StatusListener{

        void onClose();
    }
}
