package com.wxmimeprio.phoenix.reduce;

import com.wxmimeprio.phoenix.beans.DataWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataReducer extends Reducer<Text, DataWritable, NullWritable, DataWritable> {

    @Override
    protected void reduce(Text key, Iterable<DataWritable> recordings, Context context) throws IOException, InterruptedException {
        for (DataWritable recording : recordings) {
            context.write(NullWritable.get(), recording);
        }
    }
}
