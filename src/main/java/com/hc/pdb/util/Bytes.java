package com.hc.pdb.util;

public class Bytes {

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static byte[] toBytes(int val) {
        //int 占用4个字节
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            //int 强转byte，丢失高位
            b[i] = (byte) val;
            //无符号右移，无论正负，高位都以0填充
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }


    public static long toLong(byte[] b) {
        if(b == null){
            throw new IllegalArgumentException("value can not be null");
        }
        return UnsafeAccess.toLong(b, 0);
    }

    public static int toInt(byte[] b) {
        if(b == null){
            throw new IllegalArgumentException("value can not be null");
        }
        return UnsafeAccess.toInt(b, 0);
    }

    /**
     * @param bytes1
     * @param bytes2
     * @return
     */
    public static int compare(byte[] bytes1, byte[] bytes2) {
        for (int i = 0; i < bytes1.length; i++) {
            if (i > bytes2.length) {
                return 1;
            }
            int b1 = bytes1[i] & 0xff;
            int b2 = bytes2[i] & 0xff;
            if (b1 > b2) {
                return 1;
            } else if (b1 < b2) {
                return -1;
            }
            continue;
        }
        return 0;
    }
}
