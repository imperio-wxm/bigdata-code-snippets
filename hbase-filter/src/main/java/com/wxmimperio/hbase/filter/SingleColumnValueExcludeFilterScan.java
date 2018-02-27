package com.wxmimperio.hbase.filter;

import com.wxmimperio.hbase.CommonUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleColumnValueExcludeFilterScan {

    /**
     * 该过滤器继承SingleColumnValueFilter，过滤的列不会包含在结果中
     *
     * @param scan
     * @param column
     * @param condition
     * @return
     */
    public static Scan singleColumnValueExcludeFilterScan(Scan scan, String column, String condition) {
        SingleColumnValueExcludeFilter scvef = new SingleColumnValueExcludeFilter(
                Bytes.toBytes(CommonUtils.CF),
                Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(condition)
        );
        scan.setFilter(scvef);
        return scan;
    }
}
