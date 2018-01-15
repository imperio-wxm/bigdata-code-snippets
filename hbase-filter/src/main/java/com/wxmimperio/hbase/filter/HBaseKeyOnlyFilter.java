package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PageFilter;

public class HBaseKeyOnlyFilter {

    /**
     *
     * @param scan
     * @return
     */
    public static Scan keyOnlyFilterScan(Scan scan) {
        KeyOnlyFilter kof = new KeyOnlyFilter();
        scan.setFilter(kof);
        return scan;
    }
}
