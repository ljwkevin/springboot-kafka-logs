package com.demo.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * 高级 API 写起来简单
 * 
 * 不需要去自行去 管理offset，系统通过 zookeeper 自行管理
 * 
 * 不需要管理分区，副本等情况，系统自动管理
 * 
 * 消费者断线会自动根据上一次记录在 zookeeper 中的 offset去接着获取数据（默认设置1分钟更新一下 zookeeper 中存的的
 * offset）
 * 
 * 可以使用 group 来区分对同一个 topic 的不同程序访问分离开来（不同的 group 记录不同的 offset，这样不同程序读取同一个 topic
 * 才不会因为 offset 互相影响）
 *
 * @author fuhw/vencano
 * @date 2018-05-07
 */
public class KafkaHighConsumerTest {

	private static final String TOPIC_NAME = "_vencano_2018_05_07";

	private static ConsumerConnector createConsumer() {
		Properties props = new Properties();
		// properties.put("zookeeper.connect",
		// "zk1.dev.tuboshi.co:2181,zk2.dev.tuboshi.co:2181,zk3.dev.tuboshi.co:2181");
		props.put("zookeeper.connect", "localhost:2181");
		props.put("offsets.storage", "zookeeper");
		props.put("auto.offset.reset", "largest");
		props.put("auto.commit.enable", "true");
		props.put("auto.commit.interval.ms", "2000");
		props.put("group.id", "myGroup1");
		return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

	}

	public static void main(String[] args) {
		int msgCount = 0;
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(TOPIC_NAME, 1);
		ConsumerConnector consumer = createConsumer();
		Map<String, List<KafkaStream<byte[], byte[]>>> messageSteam = consumer.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> steam = messageSteam.get(TOPIC_NAME).get(0);
		ConsumerIterator<byte[], byte[]> iterator = steam.iterator();
		while (iterator.hasNext()) {
			String message = new String(iterator.next().message());
			msgCount++;
			System.err.println("consumer-msg" + msgCount + ":" + message);
		}
	}
}
