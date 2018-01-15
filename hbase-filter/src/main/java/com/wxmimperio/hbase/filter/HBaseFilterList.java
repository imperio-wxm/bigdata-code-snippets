package com.wxmimperio.hbase.filter;

import com.wxmimperio.hbase.CommonUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

public class HBaseFilterList {

    /**
     * FilterList.Operator.MUST_PASS_ALL（and）
     * FilterList.Operator.MUST_PASS_ONE（or）
     *
     * @param scan
     * @param colAndCondition
     * @return
     */
    public static Scan FilterLisScan(Scan scan, List<Map<String, String>> colAndCondition) {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        for (Map<String, String> map : colAndCondition) {
            for (Map.Entry<String, String> colAndCond : map.entrySet()) {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(
                        Bytes.toBytes(CommonUtils.CF),
                        Bytes.toBytes(colAndCond.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(colAndCond.getValue())
                );
                list.addFilter(filter);
            }
        }
        scan.setFilter(list);
        return scan;
    }
}
