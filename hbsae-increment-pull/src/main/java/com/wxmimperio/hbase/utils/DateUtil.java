package com.wxmimperio.hbase.utils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by weiximing.imperio on 2017/9/19.
 */
public class DateUtil {
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
    private static DateFormat partitionDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static DateFormat shortDateFormat = new SimpleDateFormat("yyyyMMdd");
    private static DateFormat dateTimeFormate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Date parseDateString(String dateStr) {
        int length = dateStr.length();
        SimpleDateFormat format;
        if (8 == length) {
            format = new SimpleDateFormat("yyyyMMdd");

            try {
                return format.parse(dateStr);
            } catch (ParseException var8) {
            }
        } else if (14 == length) {
            format = new SimpleDateFormat("yyyyMMddHHmmss");

            try {
                return format.parse(dateStr);
            } catch (ParseException var7) {
            }
        } else if (10 == length) {
            format = new SimpleDateFormat("yyyy-MM-dd");

            try {
                return format.parse(dateStr);
            } catch (ParseException var6) {
                format = new SimpleDateFormat("yyyy/MM/dd");

                try {
                    return format.parse(dateStr);
                } catch (ParseException var5) {
                }
            }
        } else if (19 == length) {
            format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            try {
                return format.parse(dateStr);
            } catch (ParseException var4) {
            }
        }

        throw new RuntimeException("can not parse date string :" + dateStr);
    }

    //new write.
    public static Date getOffsetDate(Date currentDate, String offsetString) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);

        int coef;
        String tmp;
        for (int i = 0; i < offsetString.length(); i++) {
            coef = 1;
            if ('-' == offsetString.charAt(i)) {
                coef = -1;
            }

            i++;
            tmp = "";
            while (offsetString.charAt(i) >= '0' && offsetString.charAt(i) <= '9') {
                tmp = tmp + offsetString.charAt(i);
                i++;
            }

            coef = coef * Integer.parseInt(tmp);

            switch (offsetString.charAt(i)) {
                case 'y':
                    calendar.add(Calendar.YEAR, coef);
                    break;
                case 'm':
                    calendar.add(Calendar.MONTH, coef);
                    break;
                case 'd':
                    calendar.add(Calendar.DATE, coef);
                    break;
                case 'h':
                    calendar.add(Calendar.HOUR, coef);
                    break;
                case 'n':
                    calendar.add(Calendar.MINUTE, coef);
                    break;
                case 's':
                    calendar.add(Calendar.SECOND, coef);
                    break;
            }
        }

        return calendar.getTime();
    }


    public static String getDatePath(Date date) {
        return dateFormat.format(date);
    }

    public static String getShortDatePath(Date date) {
        return shortDateFormat.format(date);
    }

    public static String getPartitionString(Date date) {
        return partitionDateFormat.format(date);
    }

    public static String getDateTimeString(Date date) {
        return dateTimeFormate.format(date);
    }

    public static String yesterday(Date currentDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(5, -1);
        return partitionDateFormat.format(calendar.getTime());
    }

    public static String lastMonth(Date currentDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(2, -1);
        return partitionDateFormat.format(calendar.getTime());
    }

    public static Timestamp toSQLTimestamp(String dateString) {
        return Timestamp.valueOf(dateString);
    }

    public static java.sql.Date toSQLDate(String dateString) {
        Date date = parseDateString(dateString);
        return new java.sql.Date(date.getTime());
    }

    public static java.sql.Date toSQLDate(Date date) {
        return new java.sql.Date(date.getTime());
    }


    public static String getOffsetDatePartitionString(Date currentDate, int offsetDate) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(5, offsetDate);
        return partitionDateFormat.format(calendar.getTime());
    }

    public static Date getOffsetHour(Date currentDate, int offsetHour) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDate);
        calendar.add(10, offsetHour);
        return calendar.getTime();
    }

    public static void main(String[] args) {
        Date date = new Date();
        getOffsetHour(date, -5);
        System.out.println(getOffsetHour(date, -5).toString());

        String sql = "use dw;\n" +
                "select snda_id,pt_id,sum(case when part_date='${date}' then consume_amount else 0.0 end) consume_amount,\n" +
                "       sum(case when part_date>'${date-7}' then consume_amount else 0.0 end) consume_amount_7d,\n" +
                "       sum(case when part_date>'${date-30}' then consume_amount else 0.0 end) consume_amount_30d\n" +
                "from pt_consume where PART_DATE>='${date-30}' and PART_DATE<='${date}'\n" +
                "group by snda_id,pt_id;";
        Date dataDat = DateUtil.parseDateString("20170920");
        System.out.println(replaceDateParams(sql, dataDat));
    }

    public static String replaceDateParams(String sql, Date dateDat) {
        //retain old simple: ${date-30}

        if (sql.matches("(?i)(\\$\\{date(time)?(-|\\+)\\d+\\})")) {
            sql = sql.replace("}", "d}");
        }
        Pattern pattern = Pattern.compile("(?i)(\\$\\{date(time)?(-|\\+)\\d+\\})", Pattern.DOTALL);
        while (true) {
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                String dateStr = "";
                String dateTemplate = matcher.group(1);
                dateStr = dateTemplate.replace("}", "d}");
                sql = sql.replace(dateTemplate, dateStr);
            } else {
                break;
            }
        }

        //replace ${date}
        pattern = Pattern.compile("(?i)(\\$\\{date([-+]\\d+[ymd])*\\})", Pattern.DOTALL);
        while (true) {
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                String dateStr = "";
                String dateTemplate = matcher.group(1);
                if (dateTemplate.length() > 7) {
                    String operator = dateTemplate.substring(6, dateTemplate.length() - 1);
                    dateStr = DateUtil.getPartitionString(DateUtil.getOffsetDate(dateDat, operator));
                } else {
                    dateStr = DateUtil.getPartitionString(dateDat);
                }

                sql = sql.replace(dateTemplate, dateStr);
            } else {
                break;
            }
        }
        //replace ${datetime} (while n is minutes: to distinguish from month(m) )
        pattern = Pattern.compile("(?i)(\\$\\{datetime([-+]\\d+[ymdhns])*\\})", Pattern.DOTALL);
        while (true) {
            Matcher matcher = pattern.matcher(sql);
            if (matcher.find()) {
                String dateStr = "";
                String dateTemplate = matcher.group(1);
                if (dateTemplate.length() > 11) {
                    String operator = dateTemplate.substring(10, dateTemplate.length() - 1);
                    dateStr = DateUtil.getDateTimeString(DateUtil.getOffsetDate(dateDat, operator));
                } else {
                    dateStr = DateUtil.getDateTimeString(dateDat);
                }

                sql = sql.replace(dateTemplate, dateStr);
            } else {
                break;
            }
        }

        //replace ${tempcode} which used to create tmp table identifier in hive.temp;
        sql = sql.replaceAll("\\$\\{tempcode\\}", DateUtil.getPartitionString(dateDat).replaceAll("-", ""));
        return sql;
    }
}
