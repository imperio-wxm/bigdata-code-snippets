package com.wxmimperio.hbase;

import com.google.gson.JsonObject;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;

public class CommonUtils {

    public final static String CF = "cf1";

    public static JsonObject showCell(Result result) {
        JsonObject jsonObject = new JsonObject();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            jsonObject.addProperty(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
        }
        return jsonObject;
    }
}
