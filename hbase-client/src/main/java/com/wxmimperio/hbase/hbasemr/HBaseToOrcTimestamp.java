package com.wxmimperio.hbase.hbasemr;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.pojo.HDFSFile;
import com.wxmimperio.hbase.utils.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;


public class HBaseToOrcTimestamp {
    private static Logger LOG = LoggerFactory.getLogger(HBaseToOrcTimestamp.class);

    private static String HBASE_SITE = "hbaes-site.xml";
    private static String HIVE_DB_LOCATION = "hive.db.location";
    public static String EMPTY = new String("");


    public static class HBaseMapper extends TableMapper<ImmutableBytesWritable, Text> {
        public void map(ImmutableBytesWritable row, Result value, Context context) throws InterruptedException, IOException {
            context.write(new ImmutableBytesWritable("key".getBytes()), new Text(getJsonCell(value).toString()));
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
                JsonObject jsonData = new JsonParser().parse(text.toString()).getAsJsonObject();
                for (StructField structField : fields) {
                    if (jsonData.has(structField.getFieldName())) {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, jsonData.get(structField.getFieldName()).getAsString());
                    } else {
                        HiveUtil.formatFieldValue(inspector, structField, orcStruct, EMPTY);
                    }
                }
                this.row = orcSerde.serialize(orcStruct, inspector);
                context.write(NullWritable.get(), this.row);
            }
        }
    }

    private static void runHBaseToOrc(String tableName, String partDate, String endTimestamp, String step) throws Exception {
        Configuration config = HBaseConfiguration.create();
        StructTypeInfo schema = HiveUtil.getColumnTypeDescs("dw", tableName);
        config.addResource(HBASE_SITE);
        config.set("schema", schema.getTypeName());
        config.set("orc.compress", "SNAPPY");
        config.set("mapreduce.output.basename", "orc");

        HDFSFile hdfsFile = new HDFSFile(tableName, partDate, config.get("hive.db.location"), endTimestamp, step);

        Job job = new Job(config, "HBaseToOrc");
        job.setJarByClass(HBaseToOrcTimestamp.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setTimeRange(
                Long.parseLong(hdfsFile.getStartTimestamp()),
                Long.parseLong(hdfsFile.getEndTimestamp())
        );
        scan.setMaxVersions(1);

        TableMapReduceUtil.initTableMapperJob(
                tableName,
                scan,
                HBaseMapper.class,
                null,
                null,
                job);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(HBaseReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Writable.class);
        OrcNewOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);
        job.setNumReduceTasks(1);

        FileOutputFormat.setOutputPath(job, new Path(hdfsFile.getTempPath()));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }
    }


    public static void main(String[] args) throws Exception {
        //String tableName, String partDate, String fileLocation, String endTimestamp, String step
        if (args.length != 4) {
            LOG.info("Usage: <tableName> <partDate> <endTime> <step>");
            System.exit(2);
        }
        String tableName = args[0];
        String partDate = args[1];
        String endTimestamp = args[2];
        String step = args[3];

        runHBaseToOrc(tableName, partDate, endTimestamp, step);
    }

    public static JsonObject getJsonCell(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty("Rowkey", new String(CellUtil.cloneRow(cell)));
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }


    private static List<String> getDeleteFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        List<String> fileNameList = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        int startHour = calendar.get(Calendar.HOUR_OF_DAY);
        calendar.setTime(HiveUtil.eventTomeFormat.get().parse(endTimestamp));
        int endHour = calendar.get(Calendar.HOUR_OF_DAY);
        for (int i = 0; i < (endHour - startHour); i++) {
            fileNameList.add(tableName + "_" + HiveUtil.addZero(startHour + i, 2) + "*");
        }
        return fileNameList;
    }
}
