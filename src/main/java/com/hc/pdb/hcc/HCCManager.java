package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.hc.pdb.LockContext;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.exception.PDBIOException;
import com.hc.pdb.exception.PDBRuntimeException;
import com.hc.pdb.hcc.meta.MetaReader;
import com.hc.pdb.state.HCCFileMeta;
import com.hc.pdb.state.State;
import com.hc.pdb.state.StateChangeListener;
import com.hc.pdb.util.Bytes;
import com.hc.pdb.util.RangeUtil;

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
        fileMetas.forEach((fileMeta) -> change.add(fileMeta.getFilePath()));
        // 新增
        List<String> adds = change.stream().filter((fileName) -> !have.contains(fileName)).collect(Collectors.toList());
        List<HCCFile> toAddHccFiles = new ArrayList<>();
        adds.forEach(filePath -> {
            try {
                HCCFile hccFile = new HCCFile(filePath,metaReader);
                toAddHccFiles.add(hccFile);
            } catch (PDBIOException e) {
                //todo:
                throw new PDBRuntimeException(e);
            }
        });

        // 删除
        List<String> deletes = have.stream()
                .filter(fileName -> !change.contains(fileName))
                .collect(Collectors.toList());
        // 删除删除的
        fileInfos = fileInfos.stream()
                .filter(fileInfo -> !deletes.contains(fileInfo.getFilePath()))
                .collect(Collectors.toList());

        fileInfos.addAll(toAddHccFiles);
    }

    public Set<HCCFile> searchHCCFile(byte[] startKey, byte[] endKey){
        return fileInfos.stream().filter((Predicate<HCCFile>) file ->
                RangeUtil.inOpenCloseInterval(file.getStart(),file.getEnd(),startKey,endKey)
        )
                .collect(Collectors.toSet());
    }

    @Override
    public void onChange(State state) {
        this.fileMetas = state.getFileMetas();
        loadHCCFile();
    }

}
