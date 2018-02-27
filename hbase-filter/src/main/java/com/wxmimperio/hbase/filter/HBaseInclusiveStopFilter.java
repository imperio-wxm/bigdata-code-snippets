package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseInclusiveStopFilter {

    /**
     * @param scan
     * @param endRowKey
     * @return
     */
    public static Scan inclusiveStopFilterScan(Scan scan, String endRowKey) {
        InclusiveStopFilter isf = new InclusiveStopFilter(Bytes.toBytes(endRowKey));
        scan.setFilter(isf);
        return scan;
    }
}
