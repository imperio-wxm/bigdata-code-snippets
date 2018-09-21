package com.wxmimperio.hadoop.mapreduce.reduce;

import com.wxmimperio.hadoop.utils.HiveUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class OrcReduce extends Reducer<Text, Text, NullWritable, Writable> {
    public enum Count {
        TotalCount
    }

    private final OrcSerde orcSerde = new OrcSerde();
    private Writable row;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String schemaStr = context.getConfiguration().get("schema");
        TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schemaStr);
        SettableStructObjectInspector inspector = (SettableStructObjectInspector) OrcStruct.createObjectInspector(typeInfo);
        List<StructField> fields = (List<StructField>) inspector.getAllStructFieldRefs();
        OrcStruct orcStruct = (OrcStruct) inspector.create();
        orcStruct.setNumFields(fields.size());
        for (Text text : values) {
            if ("\\|".equalsIgnoreCase(text.toString())
                    || StringUtils.isEmpty(text.toString()) ||
                    "\\N".equalsIgnoreCase(text.toString()) || "null".equalsIgnoreCase(text.toString())) {
                continue;
            }
            String[] result = text.toString().split("|", -1);
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

    public static void main(String[] args) {
        System.out.println(Arrays.asList("dfsd|dfasd|".split("\\|",-1)));
    }
}
