package com.demo.kafka.logs;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.demo.kafka.KafkaConfigUtils;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;

public class KafkaAppender<E> extends AppenderBase<E> {

	private static final Logger log = LoggerFactory.getLogger("local");
	protected Layout<E> layout;
	private Producer<String, String> producer;

	@Override
	public void start() {
		Assert.notNull(layout, "you don't set the layout of KafkaAppender");
		super.start();
		this.producer = KafkaConfigUtils.createProducer();
	}

	@Override
	public void stop() {
		super.stop();
		producer.close();
		System.out.println("[Stopping KafkaAppender !!!]");
	}

	@Override
	protected void append(E event) {
		String msg = layout.doLayout(event);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
				KafkaConfigUtils.DEFAULT_TOPIC_NAME, msg);
		System.out.println("[推送数据]:" + producerRecord);
		// Future<RecordMetadata> future = producer.send(producerRecord);
		// try {
		// // Object obj = future.get(0, TimeUnit.MILLISECONDS);//1ms内没有任何响应就直接写入文本文件
		// Object obj = future.get();
		// if (future.isDone()) {
		// System.out.println("响应结果:" + obj.toString());
		// System.out.println("推送数据到kafka成功");
		// }
		// } catch (Exception e) {
		// log.info("推送异常:{}", e.getMessage());
		// }

		producer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					exception.printStackTrace();
					log.info(msg);
				} else {
					System.out.println("[推送数据到kafka成功]:" + metadata);
				}
			}
		});
	}

	public Layout<E> getLayout() {
		return layout;
	}

	public void setLayout(Layout<E> layout) {
		this.layout = layout;
	}

}
