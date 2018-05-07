package com.demo.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class KafkaProducerTest {

	public static final String TOPIC_NAME = "_vencano_2018_05_07";


	// @formatter:off
	/**
	 * Set acknowledgements for producer requests.
	 * acks=0：意思server不会返回任何确认信息，不保证server是否收到，因为没有返回retires重试机制不会起效。
	 * acks=1：意思是partition leader已确认写record到日志中，但是不保证record是否被正确复制(建议设置1)。
	 * acks=all：意思是leader将等待所有同步复制broker的ack信息后返回。
	 * 
	 * props.put("acks", "1");
	 * 
	 * 1.If the request fails, the producer can automatically retry,
	 * 2.请设置大于0，这个重试机制与我们手动发起resend没有什么不同。 props.put("retries", 3);
	 * 
	 * 1.Specify buffer size in config 2.
	 * 10.0后product完全支持批量发送给broker，不乱你指定不同parititon，product都是批量自动发送指定parition上。 3.
	 * 当batch.size达到最大值就会触发dosend机制。 props.put("batch.size", 16384);
	 * 
	 * Reduce the no of requests less than 0;意思在指定batch.size数量没有达到情况下，在5s内也回推送数据
	 * props.put("linger.ms", 60000);
	 * 
	 * 1. The buffer.memory controls the total amount of memory available to the
	 * producer for buffering. 2. 生产者总内存被应用缓存，压缩，及其它运算。 props.put("buffer.memory",
	 * 33554432);
	 * 
	 * 可以采用的压缩方式：gzip，snappy props.put("compression.type", gzip);
	 * 
	 * 1.请保持producer，consumer 序列化方式一样，如果序列化不一样，将报错。
	 *
	 * @return
	 * @author fuhw/vencano
	 */
	// @formatter:on
	public static Producer<String, String> createProducer() {
		Properties props = new Properties();
		// props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
		// "kfk1.test.tuboshi.co:9092,kfk2.test.tuboshi.co:9092,kfk3.test.tuboshi.co:9092");
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		// props.put(ProducerConfig.RETRIES_CONFIG, 3);
		// props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		// props.put(ProducerConfig.LINGER_MS_CONFIG, 30000);
		// props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		// props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		return new KafkaProducer<String, String>(props);
	}

	public static void main(String[] args) {
		Producer<String, String> producer = createProducer();
		for (int j = 0; j < 100000; j++) {
			long now = System.currentTimeMillis();
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME,
					now + "xiaolang" + j);
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
	}

}
