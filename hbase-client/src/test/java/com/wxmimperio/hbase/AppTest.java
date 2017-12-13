package com.wxmimperio.hbase;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest {
    @org.junit.Test
    public void test() {
        String demos[] = {"1513131122812_8af4789e", "1513131122697_e17b86d4", "1513131122817_ef5614fa", "1513131122821_17fb69da"};
        Arrays.sort(demos);
        System.out.println(Arrays.asList(demos));
    }
}
