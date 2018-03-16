package com.wxmimperio.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class HiveStreaming {

    public static void main(String[] args) throws ConnectionError, InvalidPartition, InvalidTable, PartitionCreationFailed, ImpersonationFailed, InterruptedException,
            ClassNotFoundException, SerializationError, InvalidColumn, StreamingException, JSONException {

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        List<String> list = new ArrayList<String>();
        for (int i = 0; i < 1000; i++) {
            list.add(new JSONObject("{\"key\":123,\"value\":456}").toString());
        }

        System.out.println(list);

        List<String> partitionVals = new ArrayList<String>(1);
        partitionVals.add("2018-03-16");

        String[] fieldNames = new String[]{"key", "value"};

        StreamingConnection connection = null;
        TransactionBatch txnBatch = null;

        try {

            HiveEndPoint hiveEP = new HiveEndPoint("thrift://10.1.8.209:9083", "dw", "hello_acid", partitionVals);
            HiveConf hiveConf = new HiveConf();
            hiveConf.addResource("hdfs-site.xml");
            hiveConf.addResource("core-site.xml");
            hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES, true);
            hiveConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            connection = hiveEP.newConnection(true, hiveConf);
            DelimitedInputWriter writer = new DelimitedInputWriter(fieldNames, ",", hiveEP);
            txnBatch = connection.fetchTransactionBatch(10, writer);

            // Batch 1
            txnBatch.beginNextTransaction();
            for (String json : list) {
                String ret = "";
                JSONObject object = new JSONObject(json);
                for (int i = 0; i < fieldNames.length; i++) {
                    if (i == (fieldNames.length - 1)) {
                        ret += object.getString(fieldNames[i]);
                    } else {
                        ret += object.getString(fieldNames[i]) + ",";
                    }
                }
                ret += "," + UUID.randomUUID().toString();
                System.out.println(ret);
                txnBatch.write(ret.getBytes());
            }
            txnBatch.commit();

        } finally {
            if (txnBatch != null) {
                txnBatch.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}
