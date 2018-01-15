package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;

public class HBasePageFilter {

    /**
     * resultScanner.next(1)
     *
     * @param scan
     * @param pageSize
     * @return
     */
    public static Scan pageFilterScan(Scan scan, long pageSize) {
        PageFilter pf = new PageFilter(pageSize);
        scan.setFilter(pf);
        return scan;
    }
}
