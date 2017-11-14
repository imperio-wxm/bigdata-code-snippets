package com.wxmimperio.hadoop;

public class CheckFileMain {

    public static void main(String[] args) {
        String filePath = args[0];
        try {
            if (HadoopUtils.isOrcFile(filePath)) {
                System.out.println("File " + filePath + " is orcFile.");
            } else if (HadoopUtils.isSequenceFile(filePath)) {
                System.out.println("File " + filePath + " is sequenceFile.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
