package com.wxmimperio.hadoop.mapreduce;

import com.wxmimperio.hadoop.mapreduce.mapper.OrcHiveMapper;
import com.wxmimperio.hadoop.mapreduce.reduce.OrcReduce;
import com.wxmimperio.hadoop.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OrcMapreduce {

    public static void main(String[] args) throws Exception {

        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];

        Configuration conf = new Configuration();
        conf.addResource("hdfs-site.xml");
        conf.addResource("core-site.xml");

        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        conf.set("schema", schema.getTypeName());

        Job job = Job.getInstance(conf, "Mapreduce for orc file");
        job.setJarByClass(OrcMapreduce.class);
        job.setMapperClass(OrcHiveMapper.class);
        job.setReducerClass(OrcReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setInputFormatClass(OrcNewInputFormat.class);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        job.setNumReduceTasks(100);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
