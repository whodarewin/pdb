package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.ProtostuffUtils;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * 内部数据状态落地
 * @author han.congcong
 * //todo:修改成meta数据库形式。
 * Created by congcong.han on 2019/6/20.
 */
public class State implements ISerializable{

    private Set<HCCFileMeta> fileMetas = new HashSet<>();

    private Set<HCCFileMeta> compactingFileMeta = new HashSet<>();

    private WALFileMeta walFileMeta;

    public void addFileMeta(HCCFileMeta fileMeta){
        this.fileMetas.add(fileMeta);
    }

    public void delete(String fileName){
        Iterator<HCCFileMeta> fileMetaIterator = fileMetas.iterator();
        while(fileMetaIterator.hasNext()){
            if(fileMetaIterator.next().getFilePath().equals(fileName)){
                fileMetaIterator.remove();
            }
        }
    }

    public void deleteCompacting(String fileName){
        Iterator<HCCFileMeta> fileMetaIterator = compactingFileMeta.iterator();
        while(fileMetaIterator.hasNext()){
            if(fileMetaIterator.next().getFilePath().equals(fileName)){
                fileMetaIterator.remove();
            }
        }
    }

    public void addCompactingFileMeta(HCCFileMeta hccFileMeta){
        if(compactingFileMeta.contains(hccFileMeta)){
            //should not be here,if reach here,error.
            throw new RuntimeException("is in compacting,error");
        }
        this.compactingFileMeta.add(hccFileMeta);
    }

    public void setCurrentWalFileMeta(WALFileMeta meta){
        this.walFileMeta = meta;
    }

    public WALFileMeta getWalFileMeta(){
        return this.walFileMeta;
    }

    public Set<HCCFileMeta> getHccFileMetas(){
        return fileMetas;
    }

    public Set<HCCFileMeta> getCompactingFileMeta() {
        return compactingFileMeta;
    }

    public void setCompactingFileMeta(Set<HCCFileMeta> compactingFileMeta) {
        this.compactingFileMeta = compactingFileMeta;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) throws Exception {
        if(byteBuffer.limit() == 0){
            return;
        }
        byte[] intBytes = new byte[4];
        byteBuffer.get(intBytes);
        int fileCount = Bytes.toInt(intBytes);
        byteBuffer.get(intBytes);
        int dataLength = Bytes.toInt(intBytes);
        byte[] datas = new byte[dataLength];
        byteBuffer.get(datas);
        Collection collection = ProtostuffUtils.unSerializeCollection(datas);
        fileMetas = new HashSet<>();
        fileMetas.addAll(collection);
        if(fileCount != fileMetas.size()){
            throw new FileCountNotMatchException("file count in state file not " +
                    "match,header " + fileCount + " real " + fileMetas.size());
        }

        intBytes = new byte[4];
        byteBuffer.get(intBytes);
        fileCount = Bytes.toInt(intBytes);
        byteBuffer.get(intBytes);
        dataLength = Bytes.toInt(intBytes);
        datas = new byte[dataLength];
        byteBuffer.get(datas);
        collection = ProtostuffUtils.unSerializeCollection(datas);
        compactingFileMeta = new HashSet<>();
        compactingFileMeta.addAll(collection);
        if(fileCount != fileMetas.size()){
            throw new FileCountNotMatchException("compacting file count in state file not " +
                    "match,header " + fileCount + " real " + fileMetas.size());
        }

        byte[] walDatas = new byte[byteBuffer.remaining()];
        byteBuffer.get(walDatas);
        WALFileMeta walFileMeta = new WALFileMeta();
        ProtostuffUtils.unSerializeObject(walDatas, walFileMeta.getClass());
        this.walFileMeta = walFileMeta;
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        //写filemeta
        outputStream.write(Bytes.toBytes(fileMetas.size()));
        byte[] metaBytes = ProtostuffUtils.serializeCollection(fileMetas);
        outputStream.write(Bytes.toBytes(metaBytes.length));
        outputStream.write(metaBytes);
        //写compactingFileMeta
        outputStream.write(Bytes.toBytes(compactingFileMeta.size()));
        byte[] compactingFileMetaBytes = ProtostuffUtils.serializeCollection(compactingFileMeta);
        outputStream.write(Bytes.toBytes(compactingFileMetaBytes.length));
        outputStream.write(compactingFileMetaBytes);
        //写wal
        byte[] walBytes = ProtostuffUtils.serializeObject(walFileMeta);
        outputStream.write(walBytes);
        return outputStream.toByteArray();
    }

    public static class FileCountNotMatchException extends RuntimeException{
        public FileCountNotMatchException(String msg){
            super(msg);
        }
    }
}
