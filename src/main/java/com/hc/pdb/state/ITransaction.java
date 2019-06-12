package com.hc.pdb.state;

/**
 * ITransaction
 *
 * @author han.congcong
 * @date 2019/6/12
 */

public interface ITransaction {
    /**
     * 执行事务
     */
    void transaction();

    /**
     * 回滚
     */
    void rollback();
}
