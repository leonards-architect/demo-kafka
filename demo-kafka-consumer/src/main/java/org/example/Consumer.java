package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("group.id", "demo-group-1");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest");

        while (true) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                consumer.subscribe(List.of("demo_kafka"));
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("partition: " + record.partition() + " - " +
                            "key: " + record.key() + " - " +
                            "value: " + record.value());
                }
            }
        }
    }
}