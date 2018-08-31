package com.wxmimeprio.phoenix;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DataWritable implements DBWritable,Writable {

    private String stockName;

    private int year;

    private double[] recordings;

    private double maxPrice;

    @Override
    public void readFields(DataInput input) throws IOException {

    }

    @Override
    public void write(DataOutput output) throws IOException {

    }

    @Override
    public void readFields(ResultSet rs) throws SQLException {
        stockName = rs.getString("STOCK_NAME");
        year = rs.getInt("RECORDING_YEAR");
        final Array recordingsArray = rs.getArray("RECORDINGS_QUARTER");
        recordings = (double[])recordingsArray.getArray();
    }

    @Override
    public void write(PreparedStatement pstmt) throws SQLException {
        pstmt.setString(1, stockName);
        pstmt.setDouble(2, maxPrice);
    }

    public String getStockName() {
        return stockName;
    }

    public void setStockName(String stockName) {
        this.stockName = stockName;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public double[] getRecordings() {
        return recordings;
    }

    public void setRecordings(double[] recordings) {
        this.recordings = recordings;
    }

    public double getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(double maxPrice) {
        this.maxPrice = maxPrice;
    }
}
