package com.hc.pdb.state;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;

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
    void onChange(State state) throws Exception;
}
