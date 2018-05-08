package com.demo.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTest {

	public static void main(String[] args) {
		String zkServersUrl = "kfk1.test.tuboshi.co:9092,kfk2.test.tuboshi.co:9092,kfk3.test.tuboshi.co:9092";

		Producer<String, String> producer = KafkaConfigUtils.createProducer(zkServersUrl);
		for (int j = 0; j < 10; j++) {
			long now = System.currentTimeMillis();
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(
					KafkaConfigUtils.DEFAULT_TOPIC_NAME, now + "xiaolang" + j);
			Future<RecordMetadata> send = producer.send(record);
			try {
				RecordMetadata recordMetadata = send.get();
				System.err.println("push:" + recordMetadata);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}

		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

}
