package com.wxmimperio.presto;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by wxmimperio on 2018/3/4.
 */
public class DateOpts {

    public static final String DATE_FORMAT = "yyyy-MM-dd";

    @ScalarFunction("date_add")
    @Description("presto date_add function")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice dateAdd(@SqlType(StandardTypes.VARCHAR) Slice date,
                                @SqlType(StandardTypes.INTEGER) int day) throws ParseException {
        final DateFormat format = new SimpleDateFormat(DATE_FORMAT);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(format.parse(date.toString()));
        calendar.add(Calendar.DAY_OF_MONTH, (day < 0) ? -day : day);
        return Slices.utf8Slice(format.format(calendar.getTime()));
    }
}
