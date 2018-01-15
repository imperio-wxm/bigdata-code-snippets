package com.wxmimperio.hbase.filter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseFamilyFilter {


    /**
     * 可以加入比较器
     * new BinaryPrefixComparator(value) //匹配字节数组前缀
     * new RegexStringComparator(expr) // 正则表达式匹配
     * new SubstringComparator(substr)// 子字符串匹配
     * 如果希望查找的是一个已知的列族，则使用 scan.addFamily(family)  比使用过滤器效率更高
     *
     * @param scan
     * @param colFamily
     * @return
     */
    public static Scan familyFilterScan(Scan scan, String colFamily) {
        FamilyFilter ff = new FamilyFilter(
                CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes(colFamily))
        ); //表中不存在colFamily列族，过滤结果为空
        scan.setFilter(ff);
        return scan;
    }
}
