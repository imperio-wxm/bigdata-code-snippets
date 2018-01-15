package com.wxmimperio.hbase.filter;

import com.wxmimperio.hbase.CommonUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.Map;

public class SingleColumnValueFilterBytes {


    /**
     * 根据列的值来决定这一行数据是否返回，落脚点在行，而不是列。
     * 我们可以设置filter.setFilterIfMissing(true)
     * 如果为true，当这一列不存在时，不会返回，如果为false，当这一列不存在时，会返回所有的列信息
     *
     * @param scan
     * @param colAndCondition
     * @return
     */
    public static Scan SingleColumnScan(Scan scan, List<Map<String, String>> colAndCondition) {

        for (Map<String, String> map : colAndCondition) {
            for (Map.Entry<String, String> colAndCond : map.entrySet()) {
                SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                        Bytes.toBytes(CommonUtils.CF),
                        Bytes.toBytes(colAndCond.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(colAndCond.getValue())
                );
                scvf.setFilterIfMissing(true); //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回
                scan.setFilter(scvf);
            }
        }
        return scan;
    }
}
