package com.wxmimperio.hbase;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.wxmimperio.hbase.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

public class HBaseGetIncrement2 {
    private static Logger LOG = LoggerFactory.getLogger(HBaseGetIncrement2.class);

    public static String EMPTY = "";
    private static String HBASE_SITE = "hbaes-site.xml";

    public enum Count {
        TotalCount
    }

    public static class RowKeyMapper extends TableMapper<ImmutableBytesWritable, Text> {
        private static long vkey = 0L;
        private long mkey = 0L;

        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new ImmutableBytesWritable(String.valueOf(mkey).getBytes()), new Text(new String(value.getRow(), "UTF-8")));
        }
    }

    public static class RowKeyReducer extends Reducer<ImmutableBytesWritable, Text, NullWritable, Writable> {
        private static final HBaseClientOps hBaseClientOps = new HBaseClientOps();
        private final OrcSerde orcSerde = new OrcSerde();
        private Writable row;
        private String cf = "c";

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tableName = context.getConfiguration().get("tableName");
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            OrcStruct orcStruct = (OrcStruct) inspector.create();
            orcStruct.setNumFields(fields.size());

            for (Text val : values) {
                for (JSONObject data : hBaseClientOps.getDataByRowKey(tableName, val.toString(), cf)) {

                    LOG.info("=======" + data);
                    context.getCounter(Count.TotalCount).increment(1);
                }

                /*for (JSONObject data : hBaseClientOps.getDataByRowKey(tableName, val.toString(), cf)) {
                    for (StructField structField : fields) {
                        if (data.containsKey(structField.getFieldName())) {
                            HiveUtil.formatFieldValue(inspector, structField, orcStruct, data.get(structField.getFieldName()).toString());
                        } else {
                            HiveUtil.formatFieldValue(inspector, structField, orcStruct, EMPTY);
                        }
                    }
                    this.row = orcSerde.serialize(orcStruct, inspector);
                    context.write(NullWritable.get(), this.row);
                }*/
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            hBaseClientOps.close();
        }
    }

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String startTimeStamp = args[1];
        String endTimeStamp = args[2];
        String outPutPath = args[3];

        Configuration config = HBaseConfiguration.create();
        config.addResource(HBASE_SITE);
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.set("tableName", tableName);
        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        config.set("schema", schema.getTypeName());

        Scan scan = new Scan();
        scan.setCaching(1500);
        scan.setCacheBlocks(false);

        scan.setTimeRange(Long.parseLong(startTimeStamp), Long.parseLong(endTimeStamp));
        scan.setMaxVersions(1);

        Job job = new Job(config, "HBaseIncrement=" + tableName);
        job.setJarByClass(HBaseGetIncrement2.class);
        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                RowKeyMapper.class,
                NullWritable.class,
                Writable.class,
                job);

        //job.setMapOutputKeyClass(NullWritable.class);
        //job.setMapOutputValueClass(Writable.class);
        //job.setReducerClass(RowKeyReducer.class);
        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Writable.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(RowKeyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setNumReduceTasks(50);

        FileOutputFormat.setOutputPath(job, new Path(outPutPath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }
        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(Count.TotalCount);
        LOG.info("TotalCount = " + counter.getValue());
    }
}
