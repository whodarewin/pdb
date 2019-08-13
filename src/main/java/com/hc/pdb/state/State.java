package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * 内部数据状态落地
 * todo:修改成meta数据库形式.
 * @author han.congcong
 * @date 2019/6/20
 */
public class State implements ISerializable{
    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);
    private static Schema<State> schema = RuntimeSchema.getSchema(State.class);
    private static ThreadLocal<LinkedBuffer> buffer =
            ThreadLocal.withInitial(() -> LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));

    private Set<HCCFileMeta> fileMetas = new HashSet<>();

    private Set<CompactingFile> compactingFileMeta = new HashSet<>();

    private Set<WALFileMeta> flushingWals = new HashSet<>();

    private WALFileMeta walFileMeta;

    private boolean close = false;

    public Set<HCCFileMeta> getFileMetas() {
        return fileMetas;
    }

    public void setFileMetas(Set<HCCFileMeta> fileMetas) {
        this.fileMetas = fileMetas;
    }



    public Set<WALFileMeta> getFlushingWals() {
        return flushingWals;
    }

    public void setFlushingWals(Set<WALFileMeta> flushingWals) {
        this.flushingWals = flushingWals;
    }

    public WALFileMeta getWalFileMeta() {
        return walFileMeta;
    }

    public void setWalFileMeta(WALFileMeta walFileMeta) {
        this.walFileMeta = walFileMeta;
    }

    public Set<CompactingFile> getCompactingFileMeta() {
        return compactingFileMeta;
    }

    public void setCompactingFileMeta(Set<CompactingFile> compactingFileMeta) {
        this.compactingFileMeta = compactingFileMeta;
    }

    public boolean isClose() {
        return close;
    }

    public void setClose(boolean close) {
        this.close = close;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) {
        if(byteBuffer == null || byteBuffer.limit() == 0){
            return;
        }
        ProtostuffIOUtil.mergeFrom(byteBuffer.array(),this,schema);
    }

    @Override
    public byte[] serialize() {
        try{
            byte[] bytes = ProtostuffIOUtil.toByteArray(this, schema, buffer.get());
            return bytes;
        } finally {
            buffer.get().clear();
        }
    }

}
