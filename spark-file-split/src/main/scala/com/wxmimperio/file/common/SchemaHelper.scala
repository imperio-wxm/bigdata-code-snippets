package com.wxmimperio.file.common

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wxmimperio.file.common.HttpHelper.httpGet
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser

import scala.collection.mutable

object SchemaHelper {

    def getAllCacheSchema(): mutable.HashMap[String, Schema] = {
        val url = PropertiesUtils.loadProperties().getString("schema.cache.url") + "/cache/schema/getCacheAllSchema"
        val result = JSON.parseArray(httpGet(url).asString.body)
        val schemaMap = new mutable.HashMap[String, Schema]
        for (i <- 0 until result.size()) {
            val schema = new Parser().parse(result.get(i).asInstanceOf[JSONObject].toJSONString)
            schemaMap(schema.getName) = schema
        }

        schemaMap
    }
}
