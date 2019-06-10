package com.hc.pdb.hcc;

import com.google.common.base.Preconditions;
import com.hc.pdb.conf.Configuration;
import com.hc.pdb.conf.Constants;
import com.hc.pdb.exception.DBPathNotSetException;
import com.hc.pdb.file.FileConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HCCManager {
    private Configuration configuration;
    private List<HCCInfo> infos = new ArrayList<>();

    public HCCManager(Configuration configuration) {
        Preconditions.checkNotNull(configuration, "configuration can not be null");
        this.configuration = configuration;
        loadInfo();
    }

    private void loadInfo() {
        String path = configuration.get(Constants.DB_PATH_KEY);
        if (path == null) {
            throw new DBPathNotSetException();
        }

        File file = new File(path);
        if (handleEmpty(file)) {
            return;
        }

        handleNotEmpty(file);
    }

    private void handleNotEmpty(File file) {
        File[] subFiles = file.listFiles();
        for (File subFile : subFiles) {
            if (subFile.getName().endsWith(FileConstants.DATA_FILE_SUFFIX)) {
                infos.add(new HCCInfo(subFile.getName(), null, null));
            }
        }
        Collections.sort(infos, new HCCComparator());
    }

    private boolean handleEmpty(File file) {
        if (!file.exists()) {
            file.mkdirs();
            return true;
        }
        return false;
    }


}
