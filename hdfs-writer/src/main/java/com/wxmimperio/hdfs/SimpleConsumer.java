package com.wxmimperio.hdfs;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by weiximing.imperio on 2017/6/6.
 */
public class SimpleConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static String BOOTSTRAP_SERVERS = ":9092";
    private final static String topic = "";
    private final static String groupId = "kafka_hdfs";
    private final static String AUTO_OFFSET_RESET = "earliest";
    private final static int SESSION_TIMEOUT_MS = 10000;

    private Properties props() {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS);
        return props;
    }

    private KafkaConsumer<String, byte[]> getConsumer() {
        return new KafkaConsumer<String, byte[]>(props());
    }

    private void process(ConsumerRecord<String, byte[]> record) {
        System.out.println(record.partition() + "-" + record.offset());
    }

    public void start() {
        KafkaConsumer<String, byte[]> consumer = getConsumer();
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (!closed.get()) {
                ConsumerRecords<String, byte[]> records = consumer.poll(500);
                for (ConsumerRecord<String, byte[]> record : records) {
                    process(record);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        closed.set(true);
    }
}
