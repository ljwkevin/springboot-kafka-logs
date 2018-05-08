package com.demo.kafka;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;

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
public class KafkaStreamOutputTest {

	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				"kfk1.test.tuboshi.co:9092,kfk2.test.tuboshi.co:9092,kfk3.test.tuboshi.co:9092");
		props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final KStreamBuilder builder = new KStreamBuilder();
		builder.stream(KafkaProducerTest.TOPIC_NAME).flatMapValues(new ValueMapper<Object, Iterable<String>>() {
			@Override
			public Iterable<String> apply(Object value) {
				return Arrays.asList(value.toString().toUpperCase(Locale.getDefault()).split("\\W+"));
			}
		}).to(KafkaProducerTest.TOPIC_NAME);

		final KafkaStreams streams = new KafkaStreams(builder, props);
		final CountDownLatch latch = new CountDownLatch(1);
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			System.err.println(streams.allMetadata());
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
