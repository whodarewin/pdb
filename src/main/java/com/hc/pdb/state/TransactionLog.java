package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * TransactionLog
 *
 * @author han.congcong
 * @date 2019/6/12
 */

public class TransactionLog implements ISerializable {
    private long id;
    private String name;
    private String action;
    private String other;

    public TransactionLog(long id, String name, String action, String other) {
        this.id = id;
        this.name = name;
        this.action = action;
        this.other = other;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getOther() {
        return other;
    }

    public void setOther(String other) {
        this.other = other;
    }

    /**
     * todo 实现
     * @param byteBuffer
     */
    @Override
    public void deSerialize(ByteBuffer byteBuffer) {

    }

    @Override
    public byte[] serialize() throws IOException {
        return new byte[0];
    }
}
