package com.hc.pdb.state;

/**
 * StateChangeListener
 *
 * @author han.congcong
 * @date 2019/7/1
 */

public interface StateChangeListener {
    /**
     * 改变状态
     * @param state 状态
     */
    void onChange(State state);
}
