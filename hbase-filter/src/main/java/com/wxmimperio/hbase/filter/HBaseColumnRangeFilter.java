package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;


public class HBaseColumnRangeFilter {

    /**
     * minColumn - 列范围的最小值，如果为空，则没有下限
     * minColumnInclusive - 列范围是否包含minColumn
     * maxColumn - 列范围最大值，如果为空，则没有上限
     * maxColumnInclusive - 列范围是否包含maxColumn
     *
     * @param scan
     * @param startColumn
     * @param endColumn
     * @return
     */
    public static Scan columnRangeFilterScan(Scan scan, String startColumn, String endColumn) {
        ColumnRangeFilter crf = new ColumnRangeFilter(
                startColumn.getBytes(),
                true,
                endColumn.getBytes(),
                true
        );
        scan.setFilter(crf);
        return scan;
    }
}
