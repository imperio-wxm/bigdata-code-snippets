package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class HBaseFirstKeyOnlyFilter {

    /**
     *
     * @param scan
     * @return
     */
    public static Scan firstKeyOnlyFilterScan(Scan scan) {
        FirstKeyOnlyFilter fknf = new FirstKeyOnlyFilter();
        scan.setFilter(fknf);
        return scan;
    }
}
