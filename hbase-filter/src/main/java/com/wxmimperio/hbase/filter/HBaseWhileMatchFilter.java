package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;

public class HBaseWhileMatchFilter {

    /**
     * 查询到不符合的数据，查询直接结束
     *
     * @param scan
     * @param value
     * @return
     */
    public static Scan whileMatchFilterScan(Scan scan, String value) {
        ValueFilter rf = new ValueFilter(
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(value)
        );
        WhileMatchFilter wmf = new WhileMatchFilter(rf);
        scan.setFilter(wmf);
        return scan;
    }
}
