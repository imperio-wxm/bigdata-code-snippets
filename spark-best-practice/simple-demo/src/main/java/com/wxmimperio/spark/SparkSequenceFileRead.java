package com.wxmimperio.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.UUID;

public class SparkSequenceFileRead {
    private static final Logger LOG = LoggerFactory.getLogger(SparkSequenceFileRead.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Read Sequence");

        String seqFilePath = args[0];
        String outPutPath = args[1];

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            JavaPairRDD<Text, Text> pairRDD = sc.sequenceFile(seqFilePath, Text.class, Text.class);
            JavaPairRDD<Text, Text> seqRDD = pairRDD.map((Function<Tuple2<Text, Text>, Tuple2>) values -> {
                Text key = values._1;
                String value = values._2.toString();
                String[] msg = value.split("\t", -1);
                if ("791000276".equals(msg[1])) {
                    msg[1] = "xxxxx001";
                }
                return new Tuple2<>(key, new Text(StringUtils.join(msg, "\t")));
            }).mapToPair(new PairFunction<Tuple2, Text, Text>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<Text, Text> call(Tuple2 value) {
                    return new Tuple2<>((Text) value._1, (Text) value._2);
                }
            });
            seqRDD.saveAsNewAPIHadoopFile(outPutPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
            LOG.info("All size = " + seqRDD.count());
        }
    }
}
