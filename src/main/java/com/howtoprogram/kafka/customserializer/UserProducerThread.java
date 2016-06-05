package com.howtoprogram.kafka.customserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class UserProducerThread implements Runnable {

  private final KafkaProducer<String, User> producer;
  private final String topic;

  public UserProducerThread(String brokers, String topic) {
    Properties prop = createProducerConfig(brokers);
    this.producer = new KafkaProducer<String, User>(prop);
    this.topic = topic;
  }

  private static Properties createProducerConfig(String brokers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "com.howtoprogram.kafka.customserializer.UserSerializer");

    return props;
  }

  @Override
  public void run() {

    List<User> users = new ArrayList<>();
    users.add(new User(1l, "tom", "Tom", "Riddle", 40));
    users.add(new User(2l, "harry", "Harry", "Potter", 10));
    for (User user : users) {

      producer.send(new ProducerRecord<String, User>(topic, user.getUserName(), user),
          new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
              if (e != null) {
                e.printStackTrace();
              }
              System.out.println("Sent:" + user.toString());
            }
          });
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }

    // closes producer
    producer.close();

  }
}
