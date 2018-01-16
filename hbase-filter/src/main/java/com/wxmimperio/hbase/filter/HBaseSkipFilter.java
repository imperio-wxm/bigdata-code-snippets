package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;

public class HBaseSkipFilter {

    /**
     * 附加过滤器，其与ValueFilter结合使用，如果发现一行中的某一列不符合条件，那么整行就会被过滤掉
     *
     * @param scan
     * @return
     */
    public static Scan skipFilterScan(Scan scan, String value) {
        ValueFilter rf = new ValueFilter(
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator(value)
        );
        SkipFilter sf = new SkipFilter(rf);
        scan.setFilter(sf);
        return scan;
    }
}
