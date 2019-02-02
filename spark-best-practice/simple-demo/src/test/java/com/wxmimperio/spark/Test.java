package com.wxmimperio.spark;

import org.apache.commons.net.ntp.TimeStamp;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class Test {

    @org.junit.Test
    public void test001() throws Exception {
        String time = "2019-01-24 17:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(new Timestamp(simpleDateFormat.parse(time).getTime()));

        System.out.println(simpleDateFormat.parse(time).getTime());

        System.out.println(System.currentTimeMillis());

    }
}
