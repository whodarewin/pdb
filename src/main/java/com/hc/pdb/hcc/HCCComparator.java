package com.hc.pdb.hcc;

import com.hc.pdb.util.Bytes;

import java.util.Comparator;

/**
 * hcc 的内存排列顺序，按照start key 进行排序
 */
public class HCCComparator implements Comparator<HCCInfo> {

    @Override
    public int compare(HCCInfo o1, HCCInfo o2) {
        byte[] start1 = o1.getStart();
        byte[] start2 = o2.getStart();

        /**
         * todo:reader 写好了删掉
         */
        if(start1 == null || start2 == null){
            return 0;
        }

        return Bytes.compare(start1,start2);
    }
}
