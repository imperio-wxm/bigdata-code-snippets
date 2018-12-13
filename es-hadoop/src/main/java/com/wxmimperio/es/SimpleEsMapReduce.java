package com.wxmimperio.es;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import java.io.IOException;
import java.util.ResourceBundle;

public class SimpleEsMapReduce {

    public static class ReadMapper extends Mapper<Object, Text, NullWritable, Text> {
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            byte[] source = value.toString().trim().getBytes();
            context.write(NullWritable.get(), new Text(source));
        }
    }

    public static void main(String[] args) {
        try {

            String inputPath = args[0];

            ResourceBundle bundle = ResourceBundle.getBundle("application");

            Configuration conf = new Configuration();
            conf.addResource("hdfs-site.xml");
            conf.addResource("core-site.xml");
            conf.setBoolean("mapred.map.tasks.speculative.execution", false);
            conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
            // for x-pack
            conf.set("es.net.http.auth.user", bundle.getString("auth.user"));
            conf.set("es.net.http.auth.pass", bundle.getString("auth.pass"));

            conf.set("es.nodes", bundle.getString("es.nodes"));
            conf.set("es.port", bundle.getString("es.port"));
            conf.set("es.resource.write", "{table_name}$/data");
            conf.set("es.input.json", "yes");

            Job job = Job.getInstance(conf, "hadoop es write test");
            job.setJarByClass(SimpleEsMapReduce.class);
            job.setMapperClass(ReadMapper.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(EsOutputFormat.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(inputPath));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
