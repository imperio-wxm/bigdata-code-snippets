package com.wxmimperio.hbase.filter;

import com.wxmimperio.hbase.CommonUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class SingleColumnValueFilterComparable {


    public static Scan SingleColumnScan(Scan scan, Map<String, Map<String, String>> colAndCondition) {
        for (Map.Entry<String, Map<String, String>> colAndCond : colAndCondition.entrySet()) {
            for (Map.Entry<String, String> comparable : colAndCond.getValue().entrySet()) {
                ByteArrayComparable byteArrayComparable = getComparable(comparable.getKey(), comparable.getValue());
                if (byteArrayComparable == null) {
                    return scan;
                }
                SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                        Bytes.toBytes(CommonUtils.CF),
                        Bytes.toBytes(colAndCond.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        byteArrayComparable
                );
                scvf.setFilterIfMissing(true); //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回
                scan.setFilter(scvf);
            }
        }
        return scan;
    }

    private static ByteArrayComparable getComparable(String comparatorKey, String comparatorValue) {
        switch (comparatorKey) {
            case "BinaryComparator":
                return new BinaryComparator(comparatorValue.getBytes());
            case "BinaryPrefixComparator":
                return new BinaryPrefixComparator(comparatorValue.getBytes());
            case "BitComparator":
                return null;
            case "NullComparator":
                return new NullComparator();
            case "RegexStringComparator":
                return new RegexStringComparator(comparatorValue);
            case "SubstringComparator":
                return new SubstringComparator(comparatorValue);
            default:
                return null;

        }
    }
}
