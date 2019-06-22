package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.PDBConstants;
import com.hc.pdb.file.FileConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class HCCManager {
    private Configuration configuration;
    private List<HCCFile> infos = new ArrayList<>();

    public HCCManager(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "configuration can not be null");
        this.configuration = configuration;
        loadHCCFile();
    }

    private void loadHCCFile() {

    }

    public Set<HCCFile> searchHCCFile(byte[] startKey, byte[] endKey){
        return null;
    }
}
