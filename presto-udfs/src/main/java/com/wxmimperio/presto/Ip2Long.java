package com.wxmimperio.presto;

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.scalar.annotations.ScalarFunction;
import com.facebook.presto.operator.scalar.annotations.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.util.Date;

/**
 * Created by wxmimperio on 2018/2/28.
 */
public class Ip2Long {

    @ScalarFunction("ip_to_long")
    @Description("parser ip to long")
    @SqlType(StandardTypes.BIGINT)
    public static long parserIp2Long(@SqlType(StandardTypes.VARCHAR) Slice ipAddress) {
        String ip[] = ipAddress.toStringUtf8().split("\\.");
        Long ipLong = 256 * 256 * 256 * Long.parseLong(ip[0]) +
                256 * 256 * Long.parseLong(ip[1]) +
                256 * Long.parseLong(ip[2]) +
                Long.parseLong(ip[3]);
        return ipLong.longValue();
    }
}
