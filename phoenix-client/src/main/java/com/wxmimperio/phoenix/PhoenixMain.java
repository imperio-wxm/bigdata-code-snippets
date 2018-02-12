package com.wxmimperio.phoenix;

import com.wxmimperio.phoenix.connect.PhoenixConnectionPool;
import com.wxmimperio.phoenix.ops.PhoenixDDL;
import com.wxmimperio.phoenix.ops.PhoenixDDLPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


public class PhoenixMain {

    private static PhoenixDDLPool phoenixDDLPool = new PhoenixDDLPool();

    public static void main(String[] args) throws Exception {
        /*PhoenixDDL phoenixDDL = new PhoenixDDL();
        phoenixDDL.createTable("");
        phoenixDDL.closeDataSource();*/

        String createNameSpace = "CREATE SCHEMA IF NOT EXISTS dw";
        String createSql = "create table if not exists dw.wxm_test(test1 varchar not null primary key,test2 varchar)";
        String dropSql = "DROP TABLE IF EXISTS dw.wxm_test";

        phoenixDDLPool.executeSql(dropSql);
    }
}
