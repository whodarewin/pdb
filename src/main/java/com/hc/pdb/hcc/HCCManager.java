package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.State;
import com.hc.pdb.state.StateChangeListener;
import com.hc.pdb.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * HCCManager
 * @author han.congcong
 * @date 2019/7/1
 */

public class HCCManager implements StateChangeListener {
    private Configuration configuration;
    private volatile List<HCCFile> fileInfos = new ArrayList<>();
    private volatile Set<HCCFileMeta> fileMetas = new HashSet<>();
    private MetaReader metaReader;

    public HCCManager(Configuration configuration, MetaReader reader) {
        Preconditions.checkNotNull(configuration, "configuration can not be null");
        Preconditions.checkNotNull(reader,"MetaReader can not be null");
        this.configuration = configuration;
        this.metaReader = reader;
    }

    private void loadHCCFile() {
        //1 找出变动的hccFile
        List<String> have = new ArrayList<>();
        fileInfos.forEach((fileInfo) -> have.add(fileInfo.getFilePath()));
        List<String> change = new ArrayList<>();
        fileMetas.forEach((fileMeta) -> change.add(fileMeta.getFileName()));
        // 新增
        List<String> adds = change.stream().filter((fileName) -> !have.contains(fileName)).collect(Collectors.toList());
        // 删除
        List<String> deletes = have.stream().filter(fileName -> !change.contains(fileName)).collect(Collectors.toList());
        // 删除删除的
        fileInfos = fileInfos.stream().filter(fileInfo -> deletes.contains(fileInfo.getFilePath()))
                .collect(Collectors.toList());
        adds.forEach(filePath -> {
            try {
                fileInfos.add(new HCCFile(filePath,metaReader));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Set<HCCFile> searchHCCFile(byte[] startKey, byte[] endKey){
        return fileInfos.stream().filter((Predicate<HCCFile>) file -> {
            if(Bytes.compare(startKey,endKey) == 0){
                return !(Bytes.compare(startKey,file.getEnd()) > 0 || Bytes.compare(endKey,file.getStart()) < 0);
            }else{
                return !(Bytes.compare(startKey,file.getEnd()) > 0 || Bytes.compare(endKey,file.getStart()) <= 0);
            }
        })
                .collect(Collectors.toSet());
    }

    @Override
    public void onChange(State state) {
        this.fileMetas = state.getHccFileMetas();
        loadHCCFile();
    }

}
