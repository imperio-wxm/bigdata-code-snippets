package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;

public class HBaseRowFilter {

    public static Scan rowFilterScan(Scan scan, String rowValue) {
        RowFilter rf = new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(rowValue)
        );
        scan.setFilter(rf);
        return scan;
    }
}
