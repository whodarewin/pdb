package com.hc.pdb.state;

import java.util.List;
import java.util.Map;

/**
 * TransactionLogWriter
 * 事物日志管理，只负责日志管理，不保证一致性
 * @author han.congcong
 * @date 2019/6/12
 */
public class TransactionLogWriter {
    /**
     * 写LOG
     * @param log
     */
    public void write(TransactionLog log){

    }

    /**
     * 获得transaction log
     * @return
     */
    public Map<Long,List<TransactionLog>> getAllNoCompletedLog(){
        return null;
    }
}
