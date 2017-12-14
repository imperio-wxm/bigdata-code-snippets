package com.wxmimperio.hbase;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.UUID;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @org.junit.Test
    public void test() {
        String demos[] = {"900|1513248965555|eded9cd0", "900|1513248965554", "900|1513248965566"};
        Arrays.sort(demos);
        System.out.println(Arrays.asList(demos));

        UUID uuid = UUID.randomUUID();
        String logicalKey = "2017-12-13 12:00:00" + "|" + uuid.toString().substring(0, 8);
        //String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(logicalKey.hashCode() % 1000)), 3, "0") + "|" + logicalKey;

        String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 1000)).substring(0, 1) + "|" + logicalKey;


        System.out.println(rowKey);

        String start = new String("0");
        String end = "100";

        System.out.println(Integer.parseInt(end) + Integer.parseInt(start));

    }


    @org.junit.Test
    public void test1() {
        String startRegionSalt = null;
        String endRegionSalt = null;

        byte[] key = "900".getBytes();
        if (key.length == 0) {
            startRegionSalt = "000";
            endRegionSalt = String.valueOf(Integer.valueOf(startRegionSalt) + 100);
        } else {
            startRegionSalt = Bytes.toString(key).substring(0, 3);
            endRegionSalt = String.valueOf(Integer.valueOf(startRegionSalt) + 100);
        }

        //startRegionSalt = String.valueOf(Integer.valueOf(startRegionSalt) + 100);

        System.out.println(startRegionSalt);
        System.out.println(endRegionSalt);
    }
}
