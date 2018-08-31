package com.wxmimeprio.phoenix;

import com.wxmimeprio.phoenix.beans.DataWritable;
import com.wxmimeprio.phoenix.mapper.DataMapper;
import com.wxmimeprio.phoenix.reduce.DataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.phoenix.mapreduce.PhoenixOutputFormat;
import org.apache.phoenix.mapreduce.util.PhoenixMapReduceUtil;

public class PhoenixMapreduce {

    public static void main(String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("hbase-site.xml");
        final Job job = Job.getInstance(configuration, "phoenix-mr-job");

        // We can either specify a selectQuery or ignore it when we would like to retrieve all the columns
        final String selectQuery = "select MESSAGEKEY,EVENT_TIME,NAME,DATA_TIMESTAMP,TEST_ADD1,TEST_ADD2,TEST_ADD3,TEST_ADD4 from PHOENIX_APOLLO.PHOENIX_TIMESTAMP limit 10";

        // StockWritable is the DBWritable class that enables us to process the Result of the above query
        PhoenixMapReduceUtil.setInput(job, DataWritable.class, "PHOENIX_APOLLO.PHOENIX_TIMESTAMP", selectQuery);

        // Set the target Phoenix table and the columns
        PhoenixMapReduceUtil.setOutput(job, "PHOENIX_APOLLO.PHOENIX_TIMESTAMP_BAKUP", "MESSAGEKEY,EVENT_TIME,NAME,DATA_TIMESTAMP,TEST_ADD1,TEST_ADD2,TEST_ADD3,TEST_ADD4");

        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReducer.class);
        job.setOutputFormatClass(PhoenixOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DataWritable.class);
        TableMapReduceUtil.addDependencyJars(job);
        job.waitForCompletion(true);
    }
}
