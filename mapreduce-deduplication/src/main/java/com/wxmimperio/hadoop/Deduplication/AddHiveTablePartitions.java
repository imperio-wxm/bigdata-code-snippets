package com.wxmimperio.hadoop.Deduplication;

import com.wxmimperio.hadoop.Deduplication.utils.DateUtil;
import com.wxmimperio.hadoop.Deduplication.utils.HiveUtils;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AddHiveTablePartitions {
    private static final Logger LOG = LoggerFactory.getLogger(AddHiveTablePartitions.class);

    public static void main(String[] args) {
        try {
            String partDate = args[0];
            String parDateStr = DateUtil.getPartitionString(DateUtil.parseDateString(partDate));

            List<String> tables = HiveUtils.getConfigTables();
            LOG.info(String.format("Add partition = %s tables count = %s", parDateStr, tables.size()));
            LOG.info(String.format("Tables = %s", tables));
            if (CollectionUtils.isNotEmpty(tables)) {
                for (String table : tables) {
                    HiveUtils.addPartition("dw", table, parDateStr);
                }
            }
            HiveUtils.closeHiveConnect();
        } catch (Exception e) {
            LOG.error("Can not add Partitions", e);
            System.exit(1);
        }
    }
}
