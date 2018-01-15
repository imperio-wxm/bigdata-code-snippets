package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBasePrefixFilter {

    /**
     * 筛选出具有特点前缀的行键的数据
     * 扫描操作以字典序查找，当遇到比前缀大的行时，扫描结束
     *
     * @param scan
     * @param rowKeyPrefix
     * @return
     */
    public static Scan prefixFilterScan(Scan scan, String rowKeyPrefix) {
        PrefixFilter pf = new PrefixFilter(Bytes.toBytes(rowKeyPrefix));
        scan.setFilter(pf);
        return scan;
    }
}
