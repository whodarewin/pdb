package com.hc.pdb;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        File file = new File("/Users/momo/software/test");
        FileOutputStream outputStream = new FileOutputStream(file);
        outputStream.write(new byte[]{1,2,3});
        outputStream.flush();
        System.out.println(file.length());
    }
}
