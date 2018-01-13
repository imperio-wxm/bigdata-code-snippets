package com.wxmimperio.hbase;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.wxmimperio.hbase.connection.HBaseConnection;
import com.wxmimperio.hbase.filter.HBaseFilterList;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.util.List;
import java.util.Map;

public class FilterMain {

    /*{
        "area_id":"54",
            "browser_type":"chrome",
            "career":"战士",
            "career_id":"0",
            "ce":"5002239",
            "change_type":"1",
            "channel_id":"9187",
            "character_id":"54001000000023",
            "character_level":"89",
            "character_name":"S54.陶如容",
            "character_reset_times":"8",
            "create_time":"2017-08-20 10:11:51",
            "device_id":"",
            "event_time":"2017-12-19 23:56:42",
            "game_id":"791000317",
            "group_id":"1",
            "ip":"10.135.52.249",
            "login_account":"m91_13984373",
            "mid":"54-1-m91_13984373",
            "money_amount":"5.0",
            "money_type":"贡献值",
            "platform":"1",
            "publisher_id":"9187",
            "reason":"行会神技",
            "subreason":"",
            "vip_level":"7"
    }*/
    public static void main(String[] args) throws Exception {
        Connection connection = HBaseConnection.connection;

        Table table = connection.getTable(TableName.valueOf("orc_wooolh_money_consume_glog"));

        Scan scan = new Scan();

        // FilterList
        List<Map<String, String>> filterList = Lists.newArrayList();
        Map<String, String> filterOne = Maps.newHashMap();
        filterOne.put("column", "publisher_id");
        filterOne.put("condition", "9187");
        filterList.add(filterOne);

        Map<String, String> filterTwo = Maps.newHashMap();
        filterTwo.put("column", "reason");
        filterTwo.put("condition", "行会神技");
        filterList.add(filterTwo);

        scan = HBaseFilterList.FilterLisScan(scan, filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        int i = 0;
        for (Result result : resultScanner) {
            System.out.println(CommonUtils.showCell(result));
            i++;
            if (i > 4) {
                break;
            }
        }
        System.out.println("all size = " + i);
        table.close();
    }
}
