package com.hc.pdb.util;

public class Bytes {
    public static byte[] toBytes(long val) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static byte[] toBytes(int val){
        byte [] b = new byte[4];
        for(int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }



    public static long toLong(byte[] b){
        return UnsafeAccess.toLong(b,0);
    }

    public static int toInt(byte[] b){
        return UnsafeAccess.toInt(b,0);
    }

    public static int compare(byte[] bytes1, byte[] bytes2){
        for (int i = 0; i < bytes1.length; i++) {
            if(i > bytes2.length){
                return 1;
            }
            byte b1 = bytes1[i];
            byte b2 = bytes2[i];
            if(b1 > b2){
                return 1;
            }else if (b1 < b2){
                return -1;
            }
            continue;
        }
        return 0;
    }

}
