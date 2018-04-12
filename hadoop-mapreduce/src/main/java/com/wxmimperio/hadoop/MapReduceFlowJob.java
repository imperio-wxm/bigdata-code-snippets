package com.wxmimperio.hadoop;

import org.apache.hadoop.conf.Configuration;

public class MapReduceFlowJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputPath = args[0];
        String rowKeyOutPutPath = args[1];
        String finalPath = args[2];

        // delete path
        HDFSUtils.delete(inputPath);
        HDFSUtils.delete(rowKeyOutPutPath);
        HDFSUtils.delete(finalPath);
    }
}
