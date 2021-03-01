package com.practice.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

		String bootstrapServers = "127.0.0.1:9092";

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			// create a producer record
			// 순서대로 출력되지 않음. 3개 중 랜덤한 파티션에 들어가기 때문.
			// 모든 데이터를 한 파티션에 넣고 싶으면 key를 줘야 함
			ProducerRecord<String, String> record =
				new ProducerRecord<String, String>("first_topic", "hello world " + Integer.toString(i));

			// send data - asynchronous. 백그라운드에서 일어나기 때문에 main 함수가 send 메서드가 끝날 때까지 기다려주지 않음
			producer.send(record, new Callback() {
				@Override
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record was successfully sent
						logger.info("Received new metadata. \n" +
							"Topic: " + recordMetadata.topic() + "\n" +
							"Partition: " + recordMetadata.partition() + "\n" +
							"Offset: " + recordMetadata.offset() + "\n" +
							"Timestamp: " + recordMetadata.timestamp());
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();

	}
}
