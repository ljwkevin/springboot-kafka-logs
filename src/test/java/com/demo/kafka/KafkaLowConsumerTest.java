package com.demo.kafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * 能够开发者自己控制 offset，想从哪里读取就从哪里读取。
 * 
 * 自行控制连接分区，对分区自定义进行负载均衡
 * 
 * 对 zookeeper 的依赖性降低（如：offset 不一定非要靠 zk 存储，自行存储 offset 即可，比如存在文件或者内存中）
 *
 * @author fuhw/vencano
 * @date 2018-05-07
 */
public class KafkaLowConsumerTest {

	private static KafkaConsumer<String, String> getKafkaConsumer(Properties props) {
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "myGroup");
		props.put("enable.auto.commit", "false");
		// props.put("auto.commit.interval.ms", "1000");
		// 每次poll方法调用都是client与server的一次心跳
		props.put("session.timeout.ms", "30000");
		// props.put("max.poll.records", "2");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		return new KafkaConsumer<String, String>(props);
	}

	public static void main(String[] args) {

		Properties props = new Properties();
		KafkaConsumer<String, String> consumer = getKafkaConsumer(props);
		consumer.subscribe(Arrays.asList(KafkaProducerTest.TOPIC_NAME));
		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(100);
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.println(record);
						// one by one 提交
						long lastOffset = record.offset();
						consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}
	}
}
