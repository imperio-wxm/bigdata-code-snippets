package com.wxmimperio.hbase;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.UUID;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @org.junit.Test
    public void test() {
        String demos[] = {"1513131122812_8af4789e", "1513131122697_e17b86d4", "1513131122817_ef5614fa", "1513131122821_17fb69da"};
        Arrays.sort(demos);
        System.out.println(Arrays.asList(demos));

        UUID uuid = UUID.randomUUID();
        String logicalKey = String.valueOf(System.currentTimeMillis()) + "|" + uuid.toString().substring(0, 8);
        String rowKey = StringUtils.leftPad(Integer.toString(Math.abs(logicalKey.hashCode() % 1000)), 3, "0") + "|" + logicalKey;

        System.out.println(rowKey);

    }
}
