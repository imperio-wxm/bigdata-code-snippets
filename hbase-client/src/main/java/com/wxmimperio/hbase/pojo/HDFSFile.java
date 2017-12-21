package com.wxmimperio.hbase.pojo;

import com.wxmimperio.hbase.utils.HiveUtil;

import java.text.ParseException;
import java.util.UUID;

public class HDFSFile {

    private String tableName;
    private String partDate;
    private String fileLocation;
    private String startTimestamp;
    private String endTimestamp;
    private String step;
    private String tempPath;
    private String fileName;
    private String realPath;
    private String mvPath;

    public HDFSFile(String tableName, String partDate, String fileLocation, String endTimestamp, String step) throws Exception {
        this.tableName = tableName;
        this.partDate = partDate;
        this.fileLocation = fileLocation;
        this.endTimestamp = endTimestamp;
        this.step = step;
        initPathParams();
    }

    private void initPathParams() throws Exception {
        this.tempPath = HiveUtil.getTempPath(tableName, partDate, fileLocation);
        this.startTimestamp = HiveUtil.getStartTimestamp(endTimestamp, step);
        this.fileName = HiveUtil.getFileName(tableName, startTimestamp, endTimestamp);
        this.realPath = HiveUtil.getRealPath(tempPath, fileName);
        this.mvPath = tempPath + "orc-r-00000";
        this.endTimestamp = String.valueOf(HiveUtil.getTimestamp(endTimestamp));
    }


    public String getTableName() {
        return tableName;
    }

    public String getPartDate() {
        return partDate;
    }

    public String getFileLocation() {
        return fileLocation;
    }

    public String getStartTimestamp() {
        return startTimestamp;
    }

    public String getEndTimestamp() {
        return endTimestamp;
    }

    public String getStep() {
        return step;
    }

    public String getTempPath() {
        return tempPath;
    }

    public String getFileName() {
        return fileName;
    }

    public String getRealPath() {
        return realPath;
    }

    public String getMvPath() {
        return mvPath;
    }

    @Override
    public String toString() {
        return "HDFSFile{" +
                "tableName='" + tableName + '\'' +
                ", partDate='" + partDate + '\'' +
                ", fileLocation='" + fileLocation + '\'' +
                ", startTimestamp='" + startTimestamp + '\'' +
                ", endTimestamp='" + endTimestamp + '\'' +
                ", step='" + step + '\'' +
                ", tempPath='" + tempPath + '\'' +
                ", fileName='" + fileName + '\'' +
                ", realPath='" + realPath + '\'' +
                ", mvPath='" + mvPath + '\'' +
                '}';
    }

    public static void main(String[] args) throws Exception {
        HDFSFile hdfsFile = new HDFSFile(
                "test_table_1214",
                "2017-12-19",
                "/user/hive/warehouse/dw.db/",
                "2017-12-19 11:19:54", "-1");
        System.out.println(hdfsFile);

        System.out.println(UUID.randomUUID().toString().substring(0, 8) + new StringBuilder(String.valueOf(System.currentTimeMillis())).reverse().toString().substring(0, 4));

        System.out.println(getUUTimestamp("2017-12-20 15:25:45").substring(6, 10));
        System.out.println(getUUTimestamp("2017-12-20 15:25:45"));

        System.out.println(UUID.randomUUID());

        String logicalKey = UUID.randomUUID().toString().substring(0, 8) +
                String.valueOf(HiveUtil.eventTomeFormat.get().parse("2017-12-20 15:25:45").getTime()).substring(6, 10);
        String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 10)).substring(0, 1) + "|" + logicalKey;

        System.out.println(rowKey);

    }

    private static String getUUTimestamp(String time) throws ParseException {
        return String.valueOf(HiveUtil.eventTomeFormat.get().parse(time).getTime());
    }
}
