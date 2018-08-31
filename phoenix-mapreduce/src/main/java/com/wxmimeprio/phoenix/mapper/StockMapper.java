package com.wxmimeprio.phoenix.mapper;

import com.wxmimeprio.phoenix.beans.StockWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StockMapper extends Mapper<NullWritable, StockWritable, Text, DoubleWritable> {
    private Text stock = new Text();
    private DoubleWritable price = new DoubleWritable();

    @Override
    protected void map(NullWritable key, StockWritable stockWritable, Context context) throws IOException, InterruptedException {
        double[] recordings = stockWritable.getRecordings();
        final String stockName = stockWritable.getStockName();
        double maxPrice = Double.MIN_VALUE;
        for (double recording : recordings) {
            if (maxPrice < recording) {
                maxPrice = recording;
            }
        }
        stock.set(stockName);
        price.set(maxPrice);
        context.write(stock, price);
    }
}
