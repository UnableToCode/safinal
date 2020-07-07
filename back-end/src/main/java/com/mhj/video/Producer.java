package com.mhj.video;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
  public static final String ADDRESS_COLLECTION = "localhost:9092";
  public static final String TOPIC = "videoTransform";
  private static final Logger logger = LoggerFactory.getLogger(Producer.class.getName());
  private static KafkaProducer<String, String> producer = null;

  static {
    Properties configs = initConfig();
    producer = new KafkaProducer<String, String>(configs);
  }

  private static Properties initConfig() {
    Properties props = new Properties();
    props.put("bootstrap.servers", ADDRESS_COLLECTION);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());
    return props;
  }

  public static void sendMsg(String msg) {
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, msg);
    producer.send(
        record,
        (recordMetadata, e) -> {
          if (null != e) {
            logger.error("send error" + e.getMessage());
          } else {
            logger.info(
                String.format(
                    "offset:%s,partition:%s", recordMetadata.offset(), recordMetadata.partition()));
          }
        });
  }

  public static void main(String[] args) {

    ProducerRecord<String, String> record = null;
    for (int i = 0; i < 100; i++) {
      record = new ProducerRecord<String, String>(TOPIC, "value" + i);
      producer.send(
          record,
          (recordMetadata, e) -> {
            if (null != e) {
              System.out.println("send error" + e.getMessage());
            } else {
              System.out.println(
                  String.format(
                      "offset:%s,partition:%s",
                      recordMetadata.offset(), recordMetadata.partition()));
            }
          });
    }
    producer.close();
  }
}
