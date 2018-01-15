package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQualifierFilter {

    /**
     * 可以加入比较器
     * new BinaryPrefixComparator(value) //匹配字节数组前缀
     * new RegexStringComparator(expr) // 正则表达式匹配
     * new SubstringComparator(substr)// 子字符串匹配
     *
     * @param scan
     * @param colName
     * @return
     */
    public static Scan qualifierFilterScan(Scan scan, String colName) {
        QualifierFilter qf = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(colName))
        );
        scan.setFilter(qf);
        return scan;
    }
}
