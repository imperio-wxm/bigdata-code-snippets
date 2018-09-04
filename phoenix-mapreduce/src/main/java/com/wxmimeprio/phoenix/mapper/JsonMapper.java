package com.wxmimeprio.phoenix.mapper;

import com.wxmimeprio.phoenix.beans.JsonWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JsonMapper extends Mapper<NullWritable, JsonWritable, Text, Text> {

    @Override
    protected void map(NullWritable key, JsonWritable jsonWritable, Context context) throws IOException, InterruptedException {
        System.out.println(jsonWritable.getData());
        String eventTime = jsonWritable.getData().get(0).getAsJsonObject().get("value").getAsString();
        String msgKey = jsonWritable.getData().get(1).getAsJsonObject().get("value").getAsString();
        context.write(new Text(eventTime + msgKey), new Text(jsonWritable.getData().toString()));
    }
}
