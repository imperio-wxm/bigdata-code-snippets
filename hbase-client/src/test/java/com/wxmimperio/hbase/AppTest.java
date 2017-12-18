package com.wxmimperio.hbase;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
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

        for (int i = 0; i < 30; i++) {


            UUID uuid = UUID.randomUUID();
            String logicalKey = System.currentTimeMillis() + "|" + uuid.toString().substring(0, 8);
            //String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(logicalKey.hashCode() % 1000)), 3, "0") + "|" + logicalKey;

            String rowKey = Integer.toString(Math.abs(logicalKey.hashCode() % 10)) + "|" + logicalKey;


            System.out.println(rowKey);
        }

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

        System.out.println(encodeHexStr(15, "1|1513564850774|dbb17da1"));
    }

    public static String encodeHexStr(int dataCoding, String realStr) {
        String hexStr = null;
        if (realStr != null) {
            try {
                if (dataCoding == 15) {
                    hexStr = new String(Hex.encodeHex(realStr.getBytes("UTF-8")));
                } else if ((dataCoding & 0x0C) == 0x08) {
                    hexStr = new String(Hex.encodeHex(realStr.getBytes("UnicodeBigUnmarked")));
                } else {
                    hexStr = new String(Hex.encodeHex(realStr.getBytes("ISO8859-1")));
                }
            } catch (UnsupportedEncodingException e) {
                System.out.println(e.toString());
            }
        }
        return hexStr;
    }

    public static String hexString2String(String src) {
        String temp = "";
        for (int i = 0; i < src.length() / 2; i++) {
            temp = temp + (char) Integer.valueOf(src.substring(i * 2, i * 2 + 2),
                    16).byteValue();
        }
        return temp;
    }
}
