package com.wxmimeprio.phoenix.reduce;

import com.wxmimeprio.phoenix.DataWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataReducer extends Reducer<Text, DoubleWritable, NullWritable, DataWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> recordings, Context context) throws IOException, InterruptedException {
        double maxPrice = Double.MIN_VALUE;
        for (DoubleWritable recording : recordings) {
            if (maxPrice < recording.get()) {
                maxPrice = recording.get();
            }
        }
        final DataWritable stock = new DataWritable();
        stock.setStockName(key.toString());
        stock.setMaxPrice(maxPrice);
        context.write(NullWritable.get(), stock);
    }
}
