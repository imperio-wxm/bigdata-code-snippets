package com.wxmimperio.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkSequenceFileRead {
    private static final Logger LOG = LoggerFactory.getLogger(SparkSequenceFileRead.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Read Sequence");

        String seqFilePath = args[0];
        String outPutPath = args[1];

        List<String> keys = new ArrayList<>();
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<Text, Text> pairRDD = sc.sequenceFile(seqFilePath, Text.class, Text.class);
            JavaRDD<Tuple2> tranRDD = pairRDD.map((Function<Tuple2<Text, Text>, Tuple2>) values -> {
                Text key = values._1;
                String value = values._2.toString();
                String[] msg = value.split("\t", -1);
                LOG.info("Msg = " + Arrays.asList(msg));
                if ("791000276".equals(msg[1])) {
                    msg[1] = "xxxxx001";
                    keys.add(key.toString());
                }
                return new Tuple2<>(key, new Text(StringUtils.join(msg, "\t")));
            });
            List<Tuple2> list = tranRDD.collect();

            LOG.info("tran size = " + keys.size());
            LOG.info("Finish size = " + list.size());
            //tranRDD.saveAsObjectFile(outPutPath);
        }
    }
}
