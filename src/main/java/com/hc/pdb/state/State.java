package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import com.hc.pdb.util.Bytes;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * pdb数据文件状态管理
 * Created by congcong.han on 2019/6/20.
 */
public class State implements ISerializable{
    private Schema<FileMeta> schema = RuntimeSchema.getSchema(FileMeta.class);

    private LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

    private Set<FileMeta> fileMetas = new HashSet<>();

    public void addFileName(FileMeta fileMeta){
        this.fileMetas.add(fileMeta);
    }

    public void delete(String fileName){
        this.fileMetas.remove(fileName);
    }

    public Set<FileMeta> getHccFileNames(){
        return fileMetas;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) throws UnsupportedEncodingException {
        byte[] fileCountBytes = new byte[4];
        byteBuffer.get(fileCountBytes);
        int fileCount = Bytes.toInt(fileCountBytes);
        while(byteBuffer.position() < byteBuffer.limit()){
            byteBuffer.get(fileCountBytes);
            int length = Bytes.toInt(fileCountBytes);
            byte[] metaBytes = new byte[length];
            byteBuffer.get(metaBytes);
            FileMeta meta = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(metaBytes, meta, schema);
            fileMetas.add(meta);
        }
        if(fileCount != fileMetas.size()){
            throw new FileCountNotMatchException("file count in state file not " +
                    "match,header " + fileCount + " real " + fileMetas.size());
        }
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Bytes.toBytes(fileMetas.size()));
        for(FileMeta fileMeta : fileMetas){
            byte[] data;
            try {
                data = ProtostuffIOUtil.toByteArray(fileMeta, schema, buffer);
            } finally {
                buffer.clear();
            }
            outputStream.write(Bytes.toBytes(data.length));
            outputStream.write(data);
        }
        return outputStream.toByteArray();
    }

    public static class FileCountNotMatchException extends RuntimeException{
        public FileCountNotMatchException(String msg){
            super(msg);
        }
    }
}
