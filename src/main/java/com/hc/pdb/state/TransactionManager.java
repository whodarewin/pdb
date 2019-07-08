package com.hc.pdb.state;

import java.io.RandomAccessFile;

/**
 * TransactionManager
 * 事物管理
 * 事物必须要执行成功
 * 回滚后继续执行，只能应付断电和程序突然死亡这一件事情。
 * @author han.congcong
 * @date 2019/6/12
 */

public class TransactionManager {
    private RandomAccessFile transactionLogFile;

    public TransactionManager(){

    }
}
