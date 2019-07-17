package com.hc.pdb;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * LockContext
 *
 * @author han.congcong
 * @date 2019/7/16
 */

public class LockContext {
    public static ReadWriteLock flushLock = new ReentrantReadWriteLock();
}
