package com.wxmimperio.hadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }


    @org.junit.Test
    public void test() {
        System.out.println("/app/hadoop/export2sdo_data/export2sdocertify/inc/v_mobile_app_channel_stat_day/v_mobile_app_channel_stat_day/20170709\t123\t000");
    }
}
