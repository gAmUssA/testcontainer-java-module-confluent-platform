package io.confluent.testcontainers.support;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;

import lombok.Getter;

import static java.util.Collections.singletonList;

public class TestConsumer implements MyConsumer {

  private final KafkaConsumer<String, String> consumer;

  @Getter
  private final Collection<ConsumerRecord<String, String>> receivedRecords;

  public TestConsumer(String bootstrapServer) {
    this.consumer = new KafkaConsumer<>(createConsumerProperties(bootstrapServer));
    receivedRecords = new ArrayList<>();
  }

  public void consume() {
    consumer.subscribe(singletonList("hello_world_topic"));
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
    for (ConsumerRecord<String, String> record : records) {
      System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
                        record.value());
      this.receivedRecords.add(record);
    }
  }

  public static void main(String[] args) {
    TestConsumer helloConsumer = new TestConsumer("localhost:29092");
    helloConsumer.consume();
  }


  @NotNull
  public Properties createConsumerProperties(String bootstrapServer) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup" + new Random().nextInt());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return props;
  }
}
