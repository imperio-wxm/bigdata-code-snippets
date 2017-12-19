package com.wxmimperio.hbase.hbasemr;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wxmimperio.hbase.utils.HDFSUtils;
import com.wxmimperio.hbase.utils.HiveUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HBaseToOrcSplit {
    private static Logger LOG = LoggerFactory.getLogger(HBaseToOrcSplit.class);

    private static String HBASE_SITE = "hbaes-site.xml";
    private static String HIVE_DB_LOCATION = "hive.db.location";
    private static String EMPTY = new String("");

    public static final ThreadLocal<SimpleDateFormat> eventTomeFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
    };

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
                LOG.info("message = " + jsonData.toString());
                for (StructField structField : fields) {
                    if (jsonData.has(structField.getFieldName())) {
                        HiveUtils.formatFieldValue(inspector, structField, orcStruct, jsonData.get(structField.getFieldName()).getAsString());
                    } else {
                        HiveUtils.formatFieldValue(inspector, structField, orcStruct, EMPTY);
                    }
                }
                this.row = orcSerde.serialize(orcStruct, inspector);
                context.write(NullWritable.get(), this.row);
            }
        }
    }

    private static void runHBaseToOrc(String tableName, long startTimestamp, long endTimestamp, String outputPath) throws Exception {
        Configuration config = HBaseConfiguration.create();
        StructTypeInfo schema = HiveUtils.getColumnTypeDescs("dw", tableName);
        config.set("schema", schema.getTypeName());
        config.set("orc.compress", "SNAPPY");

        config.addResource(HBASE_SITE);
        Job job = new Job(config, "HBaseToOrc");
        job.setJarByClass(HBaseToOrcSplit.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setTimeRange(startTimestamp, endTimestamp);
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

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.addResource(HBASE_SITE);

        String tableName = args[0];
        long startTimestamp = Long.parseLong(args[1]);
        long endTimestamp = Long.parseLong(args[2]);
        String outputPath = args[3];

        runHBaseToOrc(tableName, startTimestamp, endTimestamp, outputPath);

        /*if (args.length != 4) {
            LOG.info("Params are not right.");
            System.exit(0);
        }

        String tableName = args[0];
        String partDate = args[1];
        // 数据偏移（hour为单位）
        String step = args[2];
        String endTime = args[3];
        long startTimestamp = getStartTimestamp(endTime, step);
        String fileLocation = config.get(HIVE_DB_LOCATION);
        String fileName = getFileName(tableName, String.valueOf(startTimestamp), endTime);
        LOG.info("Config = " + config);

        Job job = new Job(config, "HBaseMapReduceRead");
        job.setJarByClass(HbaseMapReduce.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setTimeRange(getStartTimestamp(endTime, step), getTimestamp(endTime));
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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job, new Path(getFilePath(tableName, partDate, fileLocation, fileName)));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("Error with job!");
        }*/

       /* String fileNameTemp = getFileName("test_table", String.valueOf(getStartTimestamp("2017-12-19 15:59:00", "-1")), "2017-12-19 15:59:00");
        System.out.println(getFilePath("test_table", "2017-12-19", config.get(HIVE_DB_LOCATION), fileNameTemp));

        List<String> deleteFileNames = getDeleteFileName("test_table", String.valueOf(getStartTimestamp("2017-12-19 15:59:00", "-1")), "2017-12-19 15:59:00");

        for (String fileName : deleteFileNames) {
            //HDFSUtils.deleteFile(fileName);
            String deletePath = getFilePath("test_table", "2017-12-19", config.get(HIVE_DB_LOCATION), fileName);
            System.out.println(deletePath);
        }*/
    }

    public static JsonObject getJsonCell(Result value) {
        JsonObject jsonObject = new JsonObject();
        for (Cell cell : value.rawCells()) {
            jsonObject.addProperty("Rowkey", new String(CellUtil.cloneRow(cell)));
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }

    private static String getFilePath(String tableName, String partDate, String fileLocation, String fileName) throws ParseException {
        StringBuilder filePath = new StringBuilder();
        filePath.append(fileLocation)
                .append(tableName)
                .append("/part_date=")
                .append(partDate)
                .append("/")
                .append(fileName);
        return filePath.toString();
    }

    private static List<String> getDeleteFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        List<String> fileNameList = new ArrayList<String>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        int startHour = calendar.get(Calendar.HOUR_OF_DAY);
        calendar.setTime(eventTomeFormat.get().parse(endTimestamp));
        int endHour = calendar.get(Calendar.HOUR_OF_DAY);
        for (int i = 0; i < (endHour - startHour); i++) {
            fileNameList.add(tableName + "_" + addZero(startHour + i, 2) + "*");
        }
        return fileNameList;
    }

    /**
     * get file name, like test_table_11_12_1513664756483
     *
     * @param tableName
     * @param startTimestamp
     * @param endTimestamp
     * @return
     * @throws ParseException
     */
    private static String getFileName(String tableName, String startTimestamp, String endTimestamp) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(Long.parseLong(startTimestamp)));
        String startHour = addZero(calendar.get(Calendar.HOUR_OF_DAY), 2);
        calendar.setTime(eventTomeFormat.get().parse(endTimestamp));
        String endHour = addZero(calendar.get(Calendar.HOUR_OF_DAY), 2);
        return tableName + "_" + startHour + "_" + endHour + "_" + System.currentTimeMillis();
    }

    /**
     * get start timestamp by endTimestamp and step
     *
     * @param endTimestamp
     * @param step
     * @return
     * @throws ParseException
     */
    private static long getStartTimestamp(String endTimestamp, String step) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(eventTomeFormat.get().parse(endTimestamp));
        calendar.add(Calendar.HOUR_OF_DAY, Integer.parseInt(step));
        return calendar.getTime().getTime();
    }

    private static long getTimestamp(String time) throws ParseException {
        return eventTomeFormat.get().parse(time).getTime();
    }

    private static int getHour(String time) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(eventTomeFormat.get().parse(time));
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static String addZero(int num, int len) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(num);
        while (stringBuilder.length() < len) {
            stringBuilder.insert(0, "0");
        }
        return stringBuilder.toString();
    }
}
