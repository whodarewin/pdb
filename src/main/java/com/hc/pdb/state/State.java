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
 * pdb数据文件状态管理
 * Created by congcong.han on 2019/6/20.
 */
public class State implements ISerializable{

    private Set<HCCFileMeta> fileMetas = new HashSet<>();

    private List<WALFileMeta> walFileMeta = new ArrayList<>();

    public void addFileMeta(HCCFileMeta fileMeta){
        this.fileMetas.add(fileMeta);
    }

    public void delete(String fileName){

        Iterator<HCCFileMeta> fileMetaIterator = fileMetas.iterator();
        while(fileMetaIterator.hasNext()){
            if(fileMetaIterator.next().getFileName().equals(fileName)){
                fileMetaIterator.remove();
            }
        }
    }

    public void addWalFileMeta(WALFileMeta meta){
        walFileMeta.add(meta);
    }



    public Set<HCCFileMeta> getHccFileMetas(){
        return fileMetas;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) throws ClassNotFoundException, InstantiationException, IllegalAccessException, UnsupportedEncodingException {
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
        Collection collection = ProtostuffUtils.unSerialize(datas);
        fileMetas = new HashSet<>();
        fileMetas.addAll(collection);
        if(fileCount != fileMetas.size()){
            throw new FileCountNotMatchException("file count in state file not " +
                    "match,header " + fileCount + " real " + fileMetas.size());
        }

        byteBuffer.get(intBytes);
        int walCount = Bytes.toInt(intBytes);
        byteBuffer.get(intBytes);
        int walDataLength = Bytes.toInt(intBytes);
        byte[] walDatas = new byte[walDataLength];
        byteBuffer.get(walDatas);
        walFileMeta = (List<WALFileMeta>) ProtostuffUtils.unSerialize(walDatas);

        if(walCount != walFileMeta.size()){
            throw new FileCountNotMatchException("file count in state file not " +
                    "match,header " + fileCount + " real " + walFileMeta.size());
        }
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Bytes.toBytes(fileMetas.size()));
        byte[] metaBytes = ProtostuffUtils.serialize(fileMetas);
        outputStream.write(Bytes.toBytes(metaBytes.length));
        outputStream.write(metaBytes);
        outputStream.write(Bytes.toBytes(walFileMeta.size()));
        byte[] walBytes = ProtostuffUtils.serialize(walFileMeta);
        outputStream.write(Bytes.toBytes(walBytes.length));
        outputStream.write(walBytes);
        return outputStream.toByteArray();
    }

    public static class FileCountNotMatchException extends RuntimeException{
        public FileCountNotMatchException(String msg){
            super(msg);
        }
    }
}
