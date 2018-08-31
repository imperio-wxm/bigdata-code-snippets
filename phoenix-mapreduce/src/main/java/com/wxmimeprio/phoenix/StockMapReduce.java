package com.wxmimeprio.phoenix;

import com.wxmimeprio.phoenix.beans.StockWritable;
import com.wxmimeprio.phoenix.mapper.StockMapper;
import com.wxmimeprio.phoenix.reduce.StockReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

public class StockMapReduce {

    public static void main(String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("hbase-site.xml");
        final Job job = Job.getInstance(configuration, "phoenix-mr-job");

// We can either specify a selectQuery or ignore it when we would like to retrieve all the columns
        final String selectQuery = "SELECT STOCK_NAME,RECORDING_YEAR,RECORDINGS_QUARTER FROM STOCK ";

// StockWritable is the DBWritable class that enables us to process the Result of the above query
        PhoenixMapReduceUtil.setInput(job, StockWritable.class, "STOCK", selectQuery);

// Set the target Phoenix table and the columns
        PhoenixMapReduceUtil.setOutput(job, "STOCK_STATS", "STOCK_NAME,MAX_RECORDING");

        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputFormatClass(PhoenixOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(StockWritable.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.waitForCompletion(true);
    }
}
