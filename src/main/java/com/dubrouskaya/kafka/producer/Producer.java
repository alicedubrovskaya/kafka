package com.dubrouskaya.kafka;

import com.dubrouskaya.kafka.schema.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
	private static final String TOPIC = "first_topic";
	public static void main(String[] args) {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		properties.setProperty("schema.registry.url", "http://localhost:8081");

		KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
		Customer customer = Customer.newBuilder()
				.setFirstName("Ellie")
				.setLastName("Marvin")
				.setAge(24)
				.build();
		ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC, customer);
		kafkaProducer.send(producerRecord, (recordMetadata, e) -> {
			if (e == null) {
				System.out.println("Success!");
				System.out.println(recordMetadata.toString());
			} else {
				e.printStackTrace();
			}
		});
		kafkaProducer.flush();
		kafkaProducer.close();
	}

}
