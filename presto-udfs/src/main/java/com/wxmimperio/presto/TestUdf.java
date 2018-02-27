package com.wxmimperio.presto;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by wxmimperio on 2018/2/19.
 */
public class TestUdf {
    public static final String DATE_FORMAT = "yyyy-MM-dd";

    @ScalarFunction
    @Description("hive to_date function")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_date(@SqlType(StandardTypes.TIMESTAMP) long input) {
        final DateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return Slices.utf8Slice(format.format(new Date(input)));
    }
}
