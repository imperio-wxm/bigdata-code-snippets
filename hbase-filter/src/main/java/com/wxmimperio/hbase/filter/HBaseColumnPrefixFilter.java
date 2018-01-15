package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseColumnPrefixFilter {

    /**
     * 无法使用比较器
     *
     * @param scan
     * @param columnPrefix
     * @return
     */
    public static Scan columnPrefixFilterScan(Scan scan, String columnPrefix) {
        ColumnPrefixFilter cf = new ColumnPrefixFilter(Bytes.toBytes(columnPrefix));
        scan.setFilter(cf);
        return scan;
    }
}
