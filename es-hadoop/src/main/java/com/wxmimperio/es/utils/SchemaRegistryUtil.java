package com.wxmimperio.es.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.ResourceBundle;

public class SchemaRegistryUtil {

    private static ResourceBundle bundle;

    static {
        bundle = ResourceBundle.getBundle("application");
    }

    public static JSONObject getAllSchemas() throws IOException {
        JSONObject schemas = new JSONObject();
        // List all subjects
        String res = HttpClientUtil.doGet(bundle.getString("schema.registry.url") + "/subjects");
        for (JsonElement ele : new JsonParser().parse(res).getAsJsonArray()) {
            // Fetch the most recently registered schema under subject
            String subject = HttpClientUtil.doGet(bundle.getString("schema.registry.url") + "/subjects/" + ele.getAsString() + "/versions/latest");
            JsonObject json = new JsonParser().parse(subject).getAsJsonObject();
            int version = json.get("version").getAsInt();
            String schemaStr = json.get("schema").getAsString();
            Schema schema = new Schema.Parser().parse(schemaStr);
            schema.addProp("version", String.valueOf(version));
            if (!"true".equalsIgnoreCase(schema.getProp("deleted"))) {
                schemas.put(ele.getAsString(), schema.toString());
            }
        }
        return schemas;
    }
}
