package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RandomRowFilter;

public class HBaseRandomRowFilter {

    /**
     * 随机选数据
     *
     * @param scan
     * @param ratio
     * @return
     */
    public static Scan randomRowFilterScan(Scan scan, float ratio) {
        RandomRowFilter rrf = new RandomRowFilter(ratio);
        scan.setFilter(rrf);
        return scan;
    }
}
