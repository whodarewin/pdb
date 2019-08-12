package com.hc.pdb.state;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * CompactingFile
 *
 * @author han.congcong
 * @date 2019/8/12
 */

public class CompactingFile {
    public static final String BEGIN = "begin";
    public static final String WRITE_HCC_FILE_FINISH = "write_hcc_file_finish";
    public static final String ADD_COMPACTED_HCC_FILE_TO_STATE_FINISH = "add_compacted_hcc_file_to_state_finish";
    public static final String DELETE_COMPACTED_FILE_FINISH = "delete_compacted_file_finish";
    public static final String END = "end";

    private String compactingID = UUID.randomUUID().toString();
    private List<HCCFileMeta> compactingFiles = new ArrayList<>();
    private String state;
    private String toFilePath;

    public CompactingFile(){}

    public CompactingFile(List<HCCFileMeta> compactingFiles, String state, String toFilePath) {
        this.compactingFiles = compactingFiles;
        this.state = state;
        this.toFilePath = toFilePath;
    }

    public List<HCCFileMeta> getCompactingFiles() {
        return compactingFiles;
    }

    public void setCompactingFiles(List<HCCFileMeta> compactingFiles) {
        this.compactingFiles = compactingFiles;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCompactingID() {
        return compactingID;
    }

    public String getToFilePath() {
        return toFilePath;
    }

    public void setToFilePath(String toFilePath) {
        this.toFilePath = toFilePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompactingFile that = (CompactingFile) o;
        return Objects.equals(compactingID, that.compactingID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compactingID);
    }
}
