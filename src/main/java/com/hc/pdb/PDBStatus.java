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
    private volatile boolean close = false;
    private Exception crashException;
    private String cause;
    private List<StatusListener> listeners = new CopyOnWriteArrayList<>();

    public void checkDBStatus() throws DBCloseException {
        if(close){
            throw new DBCloseException(crashException);
        }
    }

    public boolean isClose() {
        return close;
    }

    public void setClose(boolean close,String cause) {
        LOGGER.info("set db to close,cause {}",cause);
        this.close = close;
        this.cause = cause;
        for (StatusListener listener : listeners) {
            try {
                listener.onClose();
            } catch (IOException e) {
                LOGGER.info("error on listener closez",e);
            }
        }
    }

    public Exception getCrashException() {
        return crashException;
    }

    public void setCrashException(Exception crashException) {
        LOGGER.error("pdb crashed",crashException);
        this.crashException = crashException;
    }

    public void addListener(StatusListener listener){
        listeners.add(listener);
    }

    public interface StatusListener{

        void onClose() throws IOException;
    }
}
