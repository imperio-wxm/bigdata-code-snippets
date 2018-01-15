package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

public class HBaseValueFilter {

    /**
     * 值过滤器用户筛选某个特定值的单元格
     *
     * @param scan
     * @param value
     * @return
     */
    public static Scan valueFilterScan(Scan scan, String value) {
        ValueFilter rf = new ValueFilter(
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(value)
        );
        scan.setFilter(rf);
        return scan;
    }
}
