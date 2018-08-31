package com.wxmimeprio.phoenix;

import com.wxmimeprio.phoenix.mapper.DataMapper;
import com.wxmimeprio.phoenix.reduce.DataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

public class PhoenixMapreduce {

    public static void main(String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        final Job job = Job.getInstance(configuration, "phoenix-mr-job");

        // We can either specify a selectQuery or ignore it when we would like to retrieve all the columns
        final String selectQuery = "SELECT STOCK_NAME,RECORDING_YEAR,RECORDINGS_QUARTER FROM STOCK ";

        // StockWritable is the DBWritable class that enables us to process the Result of the above query
        PhoenixMapReduceUtil.setInput(job, DataWritable.class, "STOCK", selectQuery);

        // Set the target Phoenix table and the columns
        PhoenixMapReduceUtil.setOutput(job, "STOCK_STATS", "STOCK_NAME,MAX_RECORDING");

        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setOutputFormatClass(PhoenixOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DataWritable.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.waitForCompletion(true);
    }
}
