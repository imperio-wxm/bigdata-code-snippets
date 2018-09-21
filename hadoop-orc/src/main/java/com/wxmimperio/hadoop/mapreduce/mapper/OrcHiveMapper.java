package com.wxmimperio.hadoop.mapreduce.mapper;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class OrcHiveMapper extends Mapper<NullWritable, OrcStruct, Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(OrcHiveMapper.class);

    private static long vkey = 0L;

    @Override
    protected void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
        vkey = vkey + 1;
        long mkey = vkey / 10000;
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
            LOG.info(stringBuilder.toString());
            context.write(new Text(String.valueOf(mkey)), new Text(stringBuilder.toString()));
        }
    }
}
