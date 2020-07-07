package com.mhj;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field.Str;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

  public static final String ADDRESS_COLLECTION = "localhost:9092";
  public static final String TOPIC = "videoTransform";
  public static final String CONSUMER_GROUP_ID = "1"; // groupId，可以分开配置
  public static final String CONSUMER_ENABLE_AUTO_COMMIT = "true"; // 是否自动提交（消费者）
  public static final String CONSUMER_AUTO_COMMIT_INTERVAL_MS = "1000";
  public static final String CONSUMER_SESSION_TIMEOUT_MS = "30000"; // 连接超时时间
  public static final int CONSUMER_MAX_POLL_RECORDS = 10; // 每次拉取数
  public static final Duration CONSUMER_POLL_TIME_OUT = Duration.ofMillis(3000); // 拉去数据超时时间

  private  static  final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
  private static KafkaConsumer<String,String> consumer;

  static {
    Properties configs = initConfig();
    consumer = new KafkaConsumer<String, String>(configs);
    consumer.subscribe(Arrays.asList(TOPIC));
  }

  private static Properties initConfig(){
    Properties props = new Properties();
    props.put("bootstrap.servers", ADDRESS_COLLECTION);
    props.put("group.id", CONSUMER_GROUP_ID);
    props.put("enable.auto.commit", CONSUMER_ENABLE_AUTO_COMMIT);
    props.put("auto.commit.interval.ms", CONSUMER_AUTO_COMMIT_INTERVAL_MS);
    props.put("session.timeout.ms", CONSUMER_SESSION_TIMEOUT_MS);
    props.put("max.poll.records", CONSUMER_MAX_POLL_RECORDS);
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    return props;
  }

  public static List<String> recvMsg(){
    List<String> msgs = new ArrayList<>();
    ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIME_OUT);
    records.forEach((ConsumerRecord<String, String> record)->{
      logger.info("revice: key ==="+record.key()+" value ===="+record.value()+" topic ==="+record.topic());
      msgs.add(record.value());
    });
    return msgs;
  }

  public static void main(String[] args) {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIME_OUT);
      records.forEach((ConsumerRecord<String, String> record)->{
        System.out.println("revice: key ==="+record.key()+" value ===="+record.value()+" topic ==="+record.topic());
      });
    }
  }

}
