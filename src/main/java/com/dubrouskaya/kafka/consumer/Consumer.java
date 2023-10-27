package com.dubrouskaya.kafka.consumer;

import com.dubrouskaya.kafka.schema.Customer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    private static final String TOPIC = "first_topic";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-consumer");
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(TOPIC));
        System.out.println("Consumer started...");

        while (true) {
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, Customer> record: records) {
                System.out.println(record.value());
            }
        }

    }
}
