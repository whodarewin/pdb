package com.hc.pdb.state;

import com.hc.pdb.ISerializable;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.util.Bytes;
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
    private Set<String> hccFileNames = new HashSet<>();

    public void addFileName(String fileName){
        this.hccFileNames.add(fileName);
    }

    public void delete(String fileName){
        this.hccFileNames.remove(fileName);
    }

    public Set<String> getHccFileNames(){
        return hccFileNames;
    }

    @Override
    public void deSerialize(ByteBuffer byteBuffer) throws UnsupportedEncodingException {
        byte[] fileCountBytes = new byte[4];
        byteBuffer.get(fileCountBytes);
        int fileCount = Bytes.toInt(fileCountBytes);
        while(byteBuffer.position() < byteBuffer.limit()){
            byteBuffer.get(fileCountBytes);
            int length = Bytes.toInt(fileCountBytes);
            byte[] nameBytes = new byte[length];
            byteBuffer.get(nameBytes);
            String fileName = new String(nameBytes,PDBConstants.Charset.UTF_8);
            hccFileNames.add(fileName);
        }
        if(fileCount != hccFileNames.size()){
            throw new FileCountNotMatchException("file count in state file not " +
                    "match,header " + fileCount + " real " + hccFileNames.size());
        }
    }

    @Override
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        outputStream.write(Bytes.toBytes(hccFileNames.size()));
        for(String fileName : hccFileNames){
            byte[] bytes = fileName.getBytes(PDBConstants.Charset.UTF_8);
            outputStream.write(Bytes.toBytes(bytes.length));
            outputStream.write(bytes);
        }
        return outputStream.toByteArray();
    }

    public static class FileCountNotMatchException extends RuntimeException{
        public FileCountNotMatchException(String msg){
            super(msg);
        }
    }
}
