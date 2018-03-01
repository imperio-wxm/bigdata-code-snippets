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
 * Created by wxmimperio on 2018/3/1.
 */
public class TimestampToDate {

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    @ScalarFunction("timestamp_to_date")
    @Description("presto to_date function")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice timestamp2Date(@SqlType(StandardTypes.TIMESTAMP) long timestamp) {
        final DateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return Slices.utf8Slice(format.format(new Date(timestamp)));
    }
}
