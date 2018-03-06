package com.wxmimperio.phoenix;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.ops.PhoenixDDL;
import com.wxmimperio.phoenix.ops.PhoenixDDLPool;
import com.wxmimperio.phoenix.ops.PhoenixDML;

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

        String insertSql = "UPSERT INTO dw.wxm_insert_test VALUES(?,?,?,?)";
        List<JSONObject> jsonObjects = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", i);
            jsonObject.put("name", "wxm" + i);
            jsonObject.put("age", i * 10);
            jsonObject.put("gender", "男");
            jsonObjects.add(jsonObject);
        }
        phoenixDML.insert(insertSql, jsonObjects);
    }
}
