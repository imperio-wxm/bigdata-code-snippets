package com.wxmimperio.hadoop.Deduplication;

import com.wxmimperio.hadoop.Deduplication.utils.HDFSUtils;
import com.wxmimperio.hadoop.Deduplication.utils.HiveUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OrcReadDeduplication {

    private static final Logger LOG = LoggerFactory.getLogger(OrcReadDeduplication.class);

    public enum Count {
        TotalCount
    }

    public static class OrcReader extends Mapper<NullWritable, OrcStruct, Text, NullWritable> {
        @Override
        protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            ObjectInspector inspector = OrcStruct.createObjectInspector(typeInfo);
            StructObjectInspector structObjectInspector = (StructObjectInspector) inspector;
            List<Object> dataAsList = structObjectInspector.getStructFieldsDataAsList(value);

            StringBuilder stringBuilder = new StringBuilder();
            if (!CollectionUtils.isEmpty(dataAsList)) {
                dataAsList.forEach(data -> {
                    String fieldValue = data == null ? "null" : data.toString();
                    stringBuilder.append(fieldValue).append("|");
                });
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                LOG.info(stringBuilder.toString());
                context.write(new Text(stringBuilder.toString()), NullWritable.get());
            }
        }
    }

    public static class OrcWriter extends Reducer<Text, NullWritable, NullWritable, Writable> {
        private final OrcSerde orcSerde = new OrcSerde();
        private Writable row;

        @Override
        protected void reduce(Text text, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String schemaStr = context.getConfiguration().get("schema");
            TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
            OrcStruct orcStruct = (OrcStruct) inspector.create();
            orcStruct.setNumFields(fields.size());

            if ("\\|".equalsIgnoreCase(text.toString())
                    || StringUtils.isEmpty(text.toString()) ||
                    "\\N".equalsIgnoreCase(text.toString()) || "null".equalsIgnoreCase(text.toString())) {
                return;
            }
            String[] result = text.toString().split("\\|", -1);
            for (int i = 0; i < fields.size(); i++) {
                try {
                    HiveUtil.formatFieldValue(inspector, fields.get(i), orcStruct, result[i]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    HiveUtil.formatFieldValue(inspector, fields.get(i), orcStruct, null);
                }
            }
            this.row = orcSerde.serialize(orcStruct, inspector);
            context.write(NullWritable.get(), this.row);
            context.getCounter(Count.TotalCount).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String tableName = args[2];

        Configuration conf = HDFSUtils.getConf();

        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        conf.set("schema", schema.getTypeName());

        Job job = Job.getInstance(conf, "Mapreduce for orc file");
        job.setJarByClass(OrcReadDeduplication.class);
        job.setMapperClass(OrcReader.class);
        job.setReducerClass(OrcWriter.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

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
