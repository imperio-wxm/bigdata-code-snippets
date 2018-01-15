package com.wxmimperio.hbase.filter;

import com.wxmimperio.hbase.CommonUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDependentColumnFilter {

    /**
     * boolean dropDependentColumn -- 决定参考列被返回还是丢弃，为true时表示参考列被返回，为false时表示被丢弃
     * CompareOp valueCompareOp --  比较运算符
     * WritableByteArrayComparable valueComparator --  比较器
     *
     * @param scan
     * @param column
     * @return
     */
    public static Scan dependentColumnFilterScan(Scan scan, String column, String condition) {
        DependentColumnFilter dcf = new DependentColumnFilter(
                Bytes.toBytes(CommonUtils.CF),
                Bytes.toBytes(column),
                false,
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(condition))
        );
        scan.setFilter(dcf);
        return scan;
    }
}
