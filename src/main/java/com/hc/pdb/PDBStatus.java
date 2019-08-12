package com.hc.pdb;

import com.hc.pdb.exception.DBCloseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * DBStatus
 *
 * @author han.congcong
 * @date 2019/8/6
 */

public class PDBStatus {
    private static final Logger LOGGER = LoggerFactory.getLogger(PDBStatus.class);
    private static volatile boolean close = false;
    private static Exception crashException;
    private static List<StatusListener> listeners = new CopyOnWriteArrayList<>();

    public static void checkDBStatus() throws DBCloseException {
        if(close){
            throw new DBCloseException(crashException);
        }
    }

    public static boolean isClose() {
        return close;
    }

    public static void setClose(boolean close) {
        LOGGER.info("set db to close");
        PDBStatus.close = close;
        for (StatusListener listener : listeners) {
            try {
                listener.onClose();
            } catch (IOException e) {
                LOGGER.info("error on listener closez",e);
            }
        }
    }

    public static Exception getCrashException() {
        return crashException;
    }

    public static void setCrashException(Exception crashException) {
        LOGGER.error("pdb crashed",crashException);
        PDBStatus.crashException = crashException;
    }

    public static void addListener(StatusListener listener){
        listeners.add(listener);
    }

    public static interface StatusListener{

        void onClose() throws IOException;
    }
}
