package com.wxmimperio.hadoop.pojo;

import org.apache.orc.TypeDescription;

public class HiveColumnTypeDesc {

    public String columnName;
    public TypeDescription td;

    public HiveColumnTypeDesc(String columnName, TypeDescription td) {
        this.columnName = columnName;
        this.td = td;
    }

    @Override
    public String toString() {
        return "HiveColumnTypeDesc{" +
                "columnName='" + columnName + '\'' +
                ", td=" + td +
                '}';
    }
}
