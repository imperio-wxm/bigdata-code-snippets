package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.TimestampsFilter;

import java.util.List;

public class HBaseTimestampFilter {

    /**
     * 使用时间戳过滤器可以对扫描结果中对版本进行细粒度的控制
     *
     * @param scan
     * @param timestamps
     * @return
     */
    public static Scan timestampFilterScan(Scan scan, List<Long> timestamps) {
        TimestampsFilter timestampsFilter = new TimestampsFilter(timestamps);
        scan.setFilter(timestampsFilter);
        return scan;
    }
}
