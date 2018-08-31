package com.wxmimeprio.phoenix.mapper;

import com.wxmimeprio.phoenix.DataWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DataMapper extends Mapper<NullWritable, DataWritable, Text, DoubleWritable> {

    private Text stock = new Text();
    private DoubleWritable price = new DoubleWritable ();

    @Override
    protected void map(NullWritable key, DataWritable dataWritable, Context context) throws IOException, InterruptedException {
        double[] recordings = dataWritable.getRecordings();
        final String stockName = dataWritable.getStockName();
        double maxPrice = Double.MIN_VALUE;
        for(double recording : recordings) {
            if(maxPrice < recording) {
                maxPrice = recording;
            }
        }
        stock.set(stockName);
        price.set(maxPrice);
        context.write(stock,price);
    }
}
