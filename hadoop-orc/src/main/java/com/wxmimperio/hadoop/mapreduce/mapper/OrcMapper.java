/*
package com.wxmimperio.hadoop.mapreduce.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OrcMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(OrcMapper.class);

    private static long vkey = 0L;

    @Override
    protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
        vkey = vkey + 1;
        long mkey = vkey / 10000;

        List<String> fieldsName = value.getSchema().getFieldNames();
        StringBuilder stringBuilder = new StringBuilder();
        fieldsName.forEach(fieldName -> {
            String fieldValue = value.getFieldValue(fieldName).toString();
            stringBuilder.append(fieldValue).append("|");
        });
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        LOG.info(fieldsName.toString());
        LOG.info(stringBuilder.toString());
        context.write(new Text(String.valueOf(mkey)), new Text(stringBuilder.toString()));
    }
}
*/
