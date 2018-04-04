package com.wxmimperio.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HFileGenerator {

    private static String HBASE_SITE = "hbaes-site.xml";

    public static class HFileMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        private static final byte[] FAMILY_BYTE = Bytes.toBytes("c");

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<String> columnNames = getColumnName(context.getConfiguration().get("columnNames"));
            String line = value.toString();
            String[] items = line.split("\t", -1);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(Bytes.toBytes(items[0]));
            for (int i = 0; i < columnNames.size(); i++) {
                Put put = new Put(rowKey.copyBytes());
                put.addColumn(FAMILY_BYTE, Bytes.toBytes(columnNames.get(i)), Bytes.toBytes(items[i]));
                context.write(rowKey, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String inputFile = args[2];
        String outputPath = args[3];

        Configuration conf = HBaseConfiguration.create();
        conf.addResource(HBASE_SITE);
        conf.set("columnNames", args[1]);

        Connection connection = ConnectionFactory.createConnection(conf);
        Job job = Job.getInstance(conf, "HFile Generator");
        job.setJarByClass(HFileGenerator.class);


        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        Table table = connection.getTable(TableName.valueOf(tableName));
        RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(tableName));

        HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));

        if (job.waitForCompletion(true)) {
            FileSystem fs = FileSystem.get(conf);
            FsPermission changedPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.setPermission(new Path(outputPath), changedPermission);
            List<String> files = getAllFilePath(new Path(outputPath), fs);
            for (String file : files) {
                fs.setPermission(new Path(file), changedPermission);
            }
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
            loader.doBulkLoad(new Path(outputPath), (HTable) table);
        }
    }

    public static List<String> getAllFilePath(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
        List<String> fileList = new ArrayList<String>();
        FileStatus[] fileStatus = fs.listStatus(filePath);
        for (FileStatus fileStat : fileStatus) {
            if (fileStat.isDirectory()) {
                fileList.add(fileStat.getPath().toString());
                fileList.addAll(getAllFilePath(fileStat.getPath(), fs));
            } else {
                fileList.add(fileStat.getPath().toString());
            }
        }
        return fileList;
    }

    private static List<String> getColumnName(String columnNames) {
        List<String> set = new ArrayList<>();
        for (String columnName : columnNames.replaceAll("\\(", "").replaceAll("\\)", "").split(",", -1)) {
            set.add(columnName);
        }
        return set;
    }
}
