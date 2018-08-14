package com.wxmimperio.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ui.SparkUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SparkDemo {
    private static final Logger LOG = LoggerFactory.getLogger(SparkDemo.class);

    public static void main(String[] args) {
        // 第一步：创建SparkConf对象,设置相关配置信息
        SparkConf conf = new SparkConf();
        conf.setAppName("wordcount");

        Tuple2<String, String>[] configs = conf.getAll();
        LOG.info("config start ================");
        Arrays.stream(configs).forEach(c -> {
            LOG.info("key" + c._1 + ", value = " + c._2);
        });
        LOG.info("config end ================");


        String sleepTime = args[0];

        // 第二步：创建JavaSparkContext对象，SparkContext是Spark的所有功能的入口
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            Option<SparkUI>  webSparkUI = sc.sc().ui();
            webSparkUI.get();
            Option<String> webUrl = sc.sc().uiWebUrl();
            LOG.info("==============================web " + webUrl.get());

            // 第三步：创建一个初始的RDD
            List<String> wordList = new ArrayList<>();
            wordList.add("While this code used the built-in support for accumulators of type Long, programmers can also create their own types by subclassing");
            wordList.add("While this code used the built-in support for accumulators of type Long, programmers can also create their own types by subclassing");
            wordList.add("While this code used the built-in support for accumulators of type Long, programmers can also create their own types by subclassing");
            JavaRDD<String> lines = sc.parallelize(wordList);

            // 第四步：对初始的RDD进行transformation操作，也就是一些计算操作
            JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterator<String> call(String line) {

                    return Arrays.asList(line.split(" ")).iterator();

                }
            });
            JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Tuple2<String, Integer> call(String word) {
                    return new Tuple2<>(word, 1);
                }
            });

            JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Integer call(Integer v1, Integer v2) {
                    return v1 + v2;
                }
            });
            wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                private static final long serialVersionUID = 1L;

                @Override
                public void call(Tuple2<String, Integer> wordCount) {
                    while (true) {
                        LOG.info(wordCount._1 + "------" + wordCount._2 + "times.");
                        try {
                            Thread.sleep(Long.parseLong(sleepTime));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
    }
}
