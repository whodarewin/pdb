package com.hc.pdb.util;

/**
 * RangeUtil
 *
 * @author han.congcong
 * @date 2019/7/15
 */

public class RangeUtil {

    public static boolean inOpenCloseInterval(byte[] rangeStart,byte[] rangeEnd,byte[] start,byte[] end){
        if(start == null && end == null){
            return true;
        }
        if(start == null){
            return Bytes.compare(end,rangeStart) > 0;
        }
        if(end == null){
            return Bytes.compare(start,rangeEnd) >= 0;
        }
        if(Bytes.compare(start,end) == 0){
            return !(Bytes.compare(start,rangeEnd) > 0 || Bytes.compare(end,rangeStart) < 0);
        }else{
            return !(Bytes.compare(end,rangeEnd) > 0 || Bytes.compare(end,rangeStart) <= 0);
        }
    }
}
