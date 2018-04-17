package com.wxmimperio.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.utils.HBaseUtil;
import com.wxmimperio.hbase.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class HBaseGetIncrement {
    public static String EMPTY = "";
    private static String HBASE_SITE = "hbaes-site.xml";

    public static class RowKeyMapper extends TableMapper<ImmutableBytesWritable, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            JSONObject jsonObject = new JSONObject();
            for (StructField structField : fields) {
                byte[] col = value.getValue(Bytes.toBytes("c"), Bytes.toBytes(structField.getFieldName()));
                jsonObject.put(structField.getFieldName(), new String(col == null ? "".getBytes() : col));
            }
            context.write(new ImmutableBytesWritable(value.getRow()), new Text(jsonObject.toJSONString()));
        }
    }

    public static class RowKeyReducer extends Reducer<ImmutableBytesWritable, Text, Writable, Writable> {
        public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, new Text(val));
            }
        }
    }


    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        private static long vkey = 0L;
        private long mkey = 0L;

        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            vkey = vkey + 1;
            mkey = vkey / 10000;
            context.write(new ImmutableBytesWritable(String.valueOf(mkey).getBytes()),
                    new Text(HiveUtil.convertResultToJson(value).get("Rowkey").getAsString()));
        }
    }

    public static class HBaseReduce extends Reducer<ImmutableBytesWritable, Text, NullWritable, Writable> {
        private final OrcSerde orcSerde = new OrcSerde();
        private Writable row;

        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            OrcStruct orcStruct = (OrcStruct) inspector.create();
            orcStruct.setNumFields(fields.size());

            for (Text text : values) {
                String message = text.toString();
                if (message.isEmpty()) {
                    continue;
                }
                JSONObject jsonObject = JSON.parseObject(message);
                for (StructField structField : fields) {
                    if (jsonObject.containsKey(structField.getFieldName())) {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, jsonObject.get(structField.getFieldName()).toString());
                    } else {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, EMPTY);
                    }
                }
                this.row = orcSerde.serialize(orcStruct, inspector);
                context.write(NullWritable.get(), this.row);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String tableName = args[0];
        String startTimeStamp = args[1];
        String endTimeStamp = args[2];
        String outPutPath = args[3];

        Configuration config = HBaseConfiguration.create();
        config.addResource(HBASE_SITE);

        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        config.set("schema", schema.getTypeName());
        config.set("tableName", tableName);

        Scan scan = new Scan();
        scan.setCaching(10000);
        scan.setCacheBlocks(false);

        scan.setTimeRange(Long.parseLong(startTimeStamp), Long.parseLong(endTimeStamp));
        scan.setMaxVersions(1);


        Job job = new Job(config, "HBaseIncrement=" + tableName);
        job.setJarByClass(HBaseGetIncrement.class);
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
        job.setReducerClass(HBaseReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(outPutPath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }

    }
}
