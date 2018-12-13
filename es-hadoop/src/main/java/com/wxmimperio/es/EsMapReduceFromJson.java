package com.wxmimperio.es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.wxmimperio.es.utils.SchemaRegistryUtil;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EsMapReduceFromJson {

    public static class TextFileReadMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Map<String, Schema> schemas = Maps.newHashMap();
        private String tableType = "";

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String allSchemaStr = context.getConfiguration().get("allSchema");
            tableType = context.getConfiguration().get("tableType").toLowerCase();
            Set<Map.Entry<String, Object>> entries = JSON.parseObject(allSchemaStr).entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                String key = entry.getKey();
                String value = (String) entry.getValue();
                schemas.put(key, new Schema.Parser().parse(value));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String message = value.toString();
            if (StringUtils.isNotEmpty(tableType) && StringUtils.isNotEmpty(message)) {
                switch (tableType) {
                    case "tlog":
                        String tablePrefix = context.getConfiguration().get("tablePrefix");
                        String[] msg = message.split("\\|", -1);
                        String tableName = tablePrefix + "_" + msg[0].toLowerCase() + "_tlog";
                        System.out.println("table name = " + tableName);
                        System.out.println("table containsKey = " + schemas.containsKey(tableName));
                        if (schemas.containsKey(tableName)) {
                            String[] newMsgs = Arrays.copyOfRange(msg, 1, msg.length);
                            Schema schema = schemas.get(tableName);
                            JSONObject json = new JSONObject();
                            List<Schema.Field> fields = schema.getFields();
                            for (int i = 0; i < newMsgs.length; i++) {
                                json.put(fields.get(i).name(), newMsgs[i]);
                            }
                            json.put("table_name", tableName);
                            System.out.println("table data = " + json.toJSONString());
                            context.write(NullWritable.get(), new Text(json.toJSONString()));
                        }
                        break;
                    case "glog":
                        break;
                    default:
                        System.exit(1);
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            String inputPath = args[0];
            String jsonOutputPath = args[1];

            Configuration conf = new Configuration();
            conf.addResource("hdfs-site.xml");
            conf.addResource("core-site.xml");

            JSONObject allSchema = SchemaRegistryUtil.getAllSchemas();
            conf.set("allSchema", allSchema.toJSONString());
            conf.set("tableType", "tlog");
            conf.set("tablePrefix", "wol3d");

            Job job = Job.getInstance(conf, "Import data to Es");
            job.setJarByClass(EsMapReduceFromJson.class);
            job.setMapperClass(TextFileReadMapper.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(jsonOutputPath));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
