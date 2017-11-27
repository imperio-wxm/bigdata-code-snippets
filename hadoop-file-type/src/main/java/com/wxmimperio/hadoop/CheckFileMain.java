package com.wxmimperio.hadoop;

public class CheckFileMain {

    public static void main(String[] args) {
        String filePath = args[0];
        String uri = args[1];
        try {
            /*if (HadoopUtils.isOrcFile(filePath)) {
                System.out.println("File " + filePath + " is orcFile.");
            } else if (HadoopUtils.isSequenceFile(filePath)) {
                System.out.println("File " + filePath + " is sequenceFile.");
            }*/
            System.out.println("File = " + filePath + " is closed ? = " + HadoopUtils.isFileClosed(uri, filePath));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
