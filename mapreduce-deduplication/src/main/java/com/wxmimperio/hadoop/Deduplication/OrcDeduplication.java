package com.wxmimperio.hadoop.Deduplication;

import com.wxmimperio.hadoop.Deduplication.utils.DateUtil;
import com.wxmimperio.hadoop.Deduplication.utils.HDFSUtils;
import com.wxmimperio.hadoop.Deduplication.utils.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.wxmimperio.hadoop.Deduplication.utils.HDFSUtils.EMPTY;

public class OrcDeduplication {
    private static final Logger LOG = LoggerFactory.getLogger(OrcDeduplication.class);

    public enum Count {
        TotalCount
    }

    public static class DeduplicationMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().equalsIgnoreCase(EMPTY)) {
                context.write(value, new Text(EMPTY));
            } else {
                context.write((Text) key, value);
            }
        }
    }

    public static class DeduplicationReducer extends Reducer<Text, Text, NullWritable, Writable> {
        private final OrcSerde orcSerde = new OrcSerde();
        private Writable row;
        private TypeInfo typeInfo;
        private SettableStructObjectInspector inspector;
        private List<StructField> fields;
        private OrcStruct orcStruct;

        @Override
        protected void setup(Context context) {
            String schemaStr = context.getConfiguration().get("schema");
            typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
            inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
            fields = (List<StructField>) inspector.getAllStructFieldRefs();
            orcStruct = (OrcStruct) inspector.create();
            orcStruct.setNumFields(fields.size());
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text value = values.iterator().next();
            String[] message = value.toString().split("\t", -1);
            for (int i = 0; i < message.length; i++) {
                if (i < fields.size()) {
                    HDFSUtils.formatFieldValue(inspector, fields.get(i), orcStruct, message[i]);
                }
            }
            this.row = orcSerde.serialize(orcStruct, inspector);
            context.write(NullWritable.get(), this.row);
            context.getCounter(Count.TotalCount).increment(1);
        }
    }


    public static void main(String[] args) {
        String tableName = args[0];
        String dataDate = args[1];
        String partDate = DateUtil.getPartitionString(DateUtil.parseDateString(dataDate));
        String inputPath = HDFSUtils.getInputPath(tableName, partDate);
        String outputPath = HDFSUtils.getOutputPath(tableName, partDate);
        String realPath = HDFSUtils.getRealPath(tableName, partDate);

        LOG.info("tableName = " + tableName);
        LOG.info("partDate = " + partDate);
        LOG.info("inputPath = " + inputPath);
        LOG.info("outputPath = " + outputPath);
        LOG.info("realPath = " + realPath);

        try {
            //delete output path
            HDFSUtils.deleteFiles(outputPath);

            int fileNum = HDFSUtils.getFileList(inputPath).size();
            if (fileNum <= 0) {
                LOG.error("Input = {} is empty, can not run job.", inputPath);
                System.exit(1);
            }
            long inputSize = HDFSUtils.getSize(inputPath);
            LOG.info("inputSize: {}", inputSize);
            int reduceNum;
            int splitNum = (int) (inputSize / (2048 * 1024 * 1024L));
            LOG.info("splitNum: {}", splitNum);
            int maxSplitNum = 100;
            if (splitNum > maxSplitNum) {
                reduceNum = 100;
            } else if (splitNum == 0) {
                reduceNum = 1;
            } else {
                reduceNum = splitNum;
            }
            LOG.info("reduce num = {}", reduceNum);

            Configuration conf = HDFSUtils.getConf();
            StructTypeInfo schema = HiveUtils.getColumnTypeDescs("dw", tableName);
            conf.set("schema", schema.getTypeName());

            Job job = Job.getInstance(conf, "Deduplication table = " + tableName);
            job.setJarByClass(OrcDeduplication.class);
            job.setMapperClass(DeduplicationMapper.class);
            job.setReducerClass(DeduplicationReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Writable.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
            job.setOutputFormatClass(OrcNewOutputFormat.class);
            job.setNumReduceTasks(reduceNum);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            if (!job.waitForCompletion(true)) {
                throw new IOException("Error with job!");
            }

            // delete output path
            HDFSUtils.deleteFiles(realPath);

            // remove file
            for (String path : HDFSUtils.getFileList(outputPath)) {
                if (HDFSUtils.isFileClosed(path)) {
                    String mvPath = HDFSUtils.getMvPath(path);
                    HDFSUtils.renameFile(path, mvPath);
                }
            }

            // delete output path
            HDFSUtils.deleteFiles(outputPath);

            Counters counters = job.getCounters();
            Counter counter = counters.findCounter(Count.TotalCount);
            LOG.info("TotalCount = {}", counter.getValue());
        } catch (Exception e) {
            LOG.error("Run job error.", e);
            System.exit(1);
        }
    }
}
