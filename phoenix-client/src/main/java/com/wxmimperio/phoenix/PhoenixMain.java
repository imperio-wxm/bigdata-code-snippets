package com.wxmimperio.phoenix;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.ops.PhoenixDDL;
import com.wxmimperio.phoenix.ops.PhoenixDDLPool;
import com.wxmimperio.phoenix.ops.PhoenixDML;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;


public class PhoenixMain {

    private static PhoenixDDLPool phoenixDDLPool = new PhoenixDDLPool();

    public static void main(String[] args) throws Exception {
        /*PhoenixDDL phoenixDDL = new PhoenixDDL();
        phoenixDDL.createTable("");
        phoenixDDL.closeDataSource();*/

        PhoenixDML phoenixDML = new PhoenixDML();

        String createNameSpace = "CREATE SCHEMA IF NOT EXISTS dw";
        String createSql = "create table if not exists dw.wxm_test(test1 varchar not null primary key /*测试列test1*/,test2 varchar /*测试列test2*/)";
        String createSql2 = "CREATE TABLE IF NOT EXISTS dw.test2 (primaryKey0 VARCHAR NOT NULL,primaryKey1 VARCHAR NOT NULL,primaryKey2 VARCHAR NOT NULL,primaryKey3 VARCHAR NOT NULL,primaryKey4 VARCHAR NOT NULL,test0 VARCHAR,test1 VARCHAR,test2 VARCHAR,test3 VARCHAR,test4 VARCHAR, CONSTRAINT pk PRIMARY KEY (primaryKey0,primaryKey1,primaryKey2,primaryKey3,primaryKey4))";
        String cretaeSql3 = "CREATE TABLE IF NOT EXISTS dw.test2 (primaryKey0 VARCHAR NOT NULL PRIMARY KEY,test0 VARCHAR /*测试0*/,test1 VARCHAR /*测试1*/,test2 VARCHAR /*测试2*/,test3 VARCHAR /*测试3*/,test4 VARCHAR /*测试4*/)COMPRESSION='SNAPPY',VERSIONS=5";
        String dropSql = "DROP TABLE IF EXISTS dw.wxm_insert_test";

        String cretaeSql4 = "CREATE TABLE IF NOT EXISTS dw.wxm_insert_test (id VARCHAR NOT NULL PRIMARY KEY,name VARCHAR,age VARCHAR,gender VARCHAR)COMPRESSION='SNAPPY',VERSIONS=1";

        //phoenixDDLPool.executeSql(cretaeSql4);

        String insertSql = "".toUpperCase();
        String filePath = "D:\\d_backup\\github\\hadoop-code-snippets\\phoenix-client\\src\\main\\resources\\data.txt";
        //System.out.println(readFileByLines(filePath));
        //phoenixDML.insert(insertSql, readFileByLines(filePath));
        String selectSql = "";
        phoenixDML.select(selectSql);
    }

    public static List<JSONObject> readFileByLines(String fileName) {
        File file = new File(fileName);
        BufferedReader reader = null;
        List<JSONObject> jsonObjects = Lists.newArrayList();
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                String[] lines = tempString.split("\t", -1);
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("MESSAGE_KEY", line);
                jsonObject.put("EVENT_TIME", lines[0]);
                jsonObject.put("GAME_ID", lines[1]);
                jsonObject.put("AREA_ID", lines[2]);
                jsonObject.put("GROUP_ID", lines[3]);
                jsonObject.put("PLATFORM", lines[4]);
                jsonObject.put("CHANNEL_ID", lines[5]);
                jsonObject.put("MID", lines[6]);
                jsonObject.put("CHARACTER_ID", lines[7]);
                jsonObject.put("CHARACTER_LEVEL", lines[8]);
                jsonObject.put("TASK_ID", lines[9]);
                jsonObject.put("TASK_TYPE", lines[10]);
                jsonObject.put("OPT_TYPE", lines[11]);
                jsonObject.put("SERIAL_NUM", lines[12]);
                jsonObjects.add(jsonObject);
                line++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
        return jsonObjects;
    }

}
