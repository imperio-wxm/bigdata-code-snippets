package com.wxmimeprio.phoenix.reduce;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimeprio.phoenix.beans.JsonWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JsonReducer extends Reducer<Text, Text, NullWritable, JsonWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> recordings, Context context) throws IOException, InterruptedException {
        for (Text recording : recordings) {
            JsonWritable jsonWritable = new JsonWritable();
            System.out.println(recording.toString());
            JsonArray json = new JsonParser().parse(recording.toString()).getAsJsonArray();
            jsonWritable.setData(json);
            context.write(NullWritable.get(), jsonWritable);
        }
    }
}
