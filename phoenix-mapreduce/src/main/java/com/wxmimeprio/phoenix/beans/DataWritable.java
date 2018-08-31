package com.wxmimeprio.phoenix.beans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataWritable implements DBWritable, Writable {

    private String messagekey;

    private String event_time;

    private String name;

    private Integer data_timestamp;

    private String test_add1;

    private String test_add2;

    private String test_add3;

    private String test_add4;

    @Override
    public void readFields(DataInput input) throws IOException {

    }

    @Override
    public void write(DataOutput output) throws IOException {

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        messagekey = rs.getString("messagekey".toUpperCase());
        event_time = rs.getString("event_time".toUpperCase());
        name = rs.getString("name".toUpperCase());
        data_timestamp = rs.getInt("data_timestamp".toUpperCase());
        test_add1 = rs.getString("test_add1".toUpperCase());
        test_add2 = rs.getString("test_add2".toUpperCase());
        test_add3 = rs.getString("test_add3".toUpperCase());
        test_add4 = rs.getString("test_add4".toUpperCase());
    }

    @Override
    public void write(PreparedStatement pstmt) throws SQLException {
        pstmt.setString(1, messagekey);
        pstmt.setString(2, event_time);
        pstmt.setString(3, name);
        pstmt.setInt(4, data_timestamp);
        pstmt.setString(5, test_add1);
        pstmt.setString(6, test_add2);
        pstmt.setString(7, test_add3);
        pstmt.setString(8, test_add4);
    }

    public String getMessagekey() {
        return messagekey;
    }

    public void setMessagekey(String messagekey) {
        this.messagekey = messagekey;
    }

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getData_timestamp() {
        return data_timestamp;
    }

    public void setData_timestamp(Integer data_timestamp) {
        this.data_timestamp = data_timestamp;
    }

    public String getTest_add1() {
        return test_add1;
    }

    public void setTest_add1(String test_add1) {
        this.test_add1 = test_add1;
    }

    public String getTest_add2() {
        return test_add2;
    }

    public void setTest_add2(String test_add2) {
        this.test_add2 = test_add2;
    }

    public String getTest_add3() {
        return test_add3;
    }

    public void setTest_add3(String test_add3) {
        this.test_add3 = test_add3;
    }

    public String getTest_add4() {
        return test_add4;
    }

    public void setTest_add4(String test_add4) {
        this.test_add4 = test_add4;
    }
}
