package com.wxmimeprio.phoenix.reduce;

import com.wxmimeprio.phoenix.beans.StockWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StockReducer extends Reducer<Text, DoubleWritable, NullWritable, StockWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> recordings, Context context) throws IOException, InterruptedException {
        double maxPrice = Double.MIN_VALUE;
        for (DoubleWritable recording : recordings) {
            if (maxPrice < recording.get()) {
                maxPrice = recording.get();
            }
        }
        final StockWritable stock = new StockWritable();
        stock.setStockName(key.toString());
        stock.setMaxPrice(maxPrice);
        context.write(NullWritable.get(), stock);
    }
}
