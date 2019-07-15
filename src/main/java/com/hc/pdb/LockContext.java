package com.hc.pdb;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LockContext
 *
 * @author han.congcong
 * @date 2019/7/16
 */

public class LockContext {
    public static Lock flushLock = new ReentrantLock();
}
