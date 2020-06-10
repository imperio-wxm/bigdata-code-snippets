package com.wxmimperio.hadoop;

public class GetFileOrPathSize {

    public static void main(String[] args) throws Exception {
        String filePath = args[0];
        String uri = args[1];

        HadoopUtils.getSize(uri, filePath);
    }
}
