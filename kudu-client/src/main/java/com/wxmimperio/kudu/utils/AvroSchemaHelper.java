package com.wxmimperio.kudu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wxmimperio.kudu.exception.KuduClientException;
import org.apache.avro.Schema;

import java.io.IOException;

public class AvroSchemaHelper {

    private static String schemaRegistryUrl;

    static {
        schemaRegistryUrl = ResourceUtils.getByString("schema.registry.url");
    }

    public static Schema getAvroSchemaByName(String schemaName) throws IOException, KuduClientException {
        String res = HttpClientUtil.doGet(schemaRegistryUrl + "/subjects");
        JSONArray avroSchemasName = JSON.parseArray(res);
        if (avroSchemasName.contains(schemaName)) {
            String subject = HttpClientUtil.doGet(schemaRegistryUrl + "/subjects/" + avroSchemasName.get(avroSchemasName.indexOf(schemaName)) + "/versions/latest");
            JSONObject json = JSON.parseObject(subject);
            int version = json.getIntValue("version");
            String schemaStr = json.getString("schema");
            Schema schema = new Schema.Parser().parse(schemaStr);
            schema.addProp("version", String.valueOf(version));
            return schema;
        } else {
            throw new KuduClientException(String.format("Avro schema = %s can not find.", schemaName));
        }
    }
}
