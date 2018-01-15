package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;

public class HBaseColumnCountGetFilter {

    /**
     * 返回每行最多返回多少列，并在遇到一定的列数超过我们锁设置的限制值的时候，结束扫描操作
     *
     * @param scan
     * @param limit
     * @return
     */
    public static Scan columnCountGetFilterScan(Scan scan, int limit) {
        ColumnCountGetFilter ccgf = new ColumnCountGetFilter(limit);
        scan.setFilter(ccgf);
        return scan;
    }
}
