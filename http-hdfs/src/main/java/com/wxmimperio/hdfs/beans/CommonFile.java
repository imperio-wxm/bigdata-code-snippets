package com.wxmimperio.hdfs.beans;

import org.springframework.stereotype.Component;

@Component
public class CommonFile {

    private String path;
    private Integer lineNum;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Integer getLineNum() {
        return lineNum;
    }

    public void setLineNum(Integer lineNum) {
        this.lineNum = lineNum;
    }

    @Override
    public String toString() {
        return "File{" +
                "path='" + path + '\'' +
                ", lineNum=" + lineNum +
                '}';
    }
}
