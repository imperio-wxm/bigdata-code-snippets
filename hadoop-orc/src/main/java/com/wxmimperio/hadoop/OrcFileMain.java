package com.wxmimperio.hadoop;

import com.google.common.collect.Lists;
import com.wxmimperio.hadoop.reader.OrcFileReader;
import com.wxmimperio.hadoop.writer.OrcFileWriter;

import java.util.Iterator;
import java.util.List;


public class OrcFileMain {

    public static void main(String[] args) throws Exception {


        List<String> datas = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            String eventTime = String.valueOf(System.currentTimeMillis());
            int game_id = i;
            int area_id = i * 2;
            int group_id = i * 3;
            String mid = "mid" + i;
            String character_id = "character_id" + i;
            int character_level = i * 4;
            String channel_id = "channel_id" + i;
            int platform = i * 5;
            String item_id = "item_id" + i;
            int item_num = i * 6;
            String reason = "reason" + i;
            String sub_reason = "sub_reason" + i;
            int change_type = i * 7;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(eventTime).append("\t")
                    .append(game_id).append("\t")
                    .append(area_id).append("\t")
                    .append(group_id).append("\t")
                    .append(mid).append("\t")
                    .append(character_id).append("\t")
                    .append(character_level).append("\t")
                    .append(channel_id).append("\t")
                    .append(platform).append("\t")
                    .append(item_id).append("\t")
                    .append(item_num).append("\t")
                    .append(reason).append("\t")
                    .append(sub_reason).append("\t")
                    .append(change_type);


            datas.add(stringBuilder.toString());

            //System.out.println(stringBuilder.toString());
        }

        // write test
        //OrcFileWriter.writeOrc("dw", "cd_item_glog", datas, "/wxm/orc_test/cd_item_glog_file", 1024);

        // read test
        OrcFileReader orcFileReader = new OrcFileReader(args[0]);
        Iterator<String> readLines = orcFileReader.iterator();
        int index = 0;
        while (readLines.hasNext()) {
            if (index++ > 10) {
                break;
            }
            System.out.println(readLines.next());
        }
        orcFileReader.close();
    }
}
