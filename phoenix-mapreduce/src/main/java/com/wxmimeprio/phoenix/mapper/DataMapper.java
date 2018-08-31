package com.wxmimeprio.phoenix.mapper;

import com.wxmimeprio.phoenix.beans.DataWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<NullWritable, DataWritable, Text, DataWritable> {

    private DataWritable data = new DataWritable();

    @Override
    protected void map(NullWritable key, DataWritable dataWritable, Context context) throws IOException, InterruptedException {
        data.setMessagekey(dataWritable.getMessagekey());
        data.setData_timestamp(dataWritable.getData_timestamp());
        data.setEvent_time(dataWritable.getEvent_time());
        data.setName(dataWritable.getName());
        data.setTest_add1(dataWritable.getTest_add1());
        data.setTest_add2(dataWritable.getTest_add2());
        data.setTest_add3(dataWritable.getTest_add3());
        data.setTest_add4(dataWritable.getTest_add4());
        context.write(new Text(dataWritable.getMessagekey()), data);
    }
}
