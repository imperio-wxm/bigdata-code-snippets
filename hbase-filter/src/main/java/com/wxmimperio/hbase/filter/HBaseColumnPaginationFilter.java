package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;

public class HBaseColumnPaginationFilter {
    /**
     * offset：类型：int32
     * 描述：起始列的位置，表示从第几列开始读。
     * limit：类型：int32
     * 描述：读取的列的个数。
     *
     * @param scan
     * @param limit
     * @param columnOffset
     * @return
     */
    public static Scan columnPaginationFilterScan(Scan scan, int limit, int columnOffset) {
        ColumnPaginationFilter cpf = new ColumnPaginationFilter(limit, columnOffset);
        scan.setFilter(cpf);
        return scan;
    }
}
