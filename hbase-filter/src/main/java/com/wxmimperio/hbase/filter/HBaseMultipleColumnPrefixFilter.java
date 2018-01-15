package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseMultipleColumnPrefixFilter {

    /**
     * MultipleColumnPrefixFilter 和 ColumnPrefixFilter 行为差不多，但可以指定多个前缀
     *
     * @param scan
     * @param columnPrefixs
     * @return
     */
    public static Scan multipleColumnPrefixFilterScan(Scan scan, byte[][] columnPrefixs) {
        MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(columnPrefixs);
        scan.setFilter(mcpf);
        return scan;
    }
}
