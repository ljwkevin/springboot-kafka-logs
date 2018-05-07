package com.demo.kafka.logs;

import java.io.StringReader;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.Layout;

public class KafkaAppender<E> extends AppenderBase<E> {

	private static final Logger log = LoggerFactory.getLogger("local");
	private static final String TOPIE_DATE_PATTERN = "yyyy-MM-dd";
	private static final String TOPIE_PRIFIX = "_vencano_";

	public static String getTopicName() {
		String format = DateFormatUtils.format(new Date(), TOPIE_DATE_PATTERN);
		return TOPIE_PRIFIX + format;
	}

	protected Layout<E> layout;
	private boolean logToLocal = false;
	private String kafkaProducerProperties;
	private KafkaProducer<String, String> producer;

	@Override
	public void start() {
		Assert.notNull(layout, "you don't set the layout of KafkaAppender");
		super.start();
		System.out.println("[Starting KafkaAppender]:");
		final Properties properties = new Properties();
		try {
			properties.load(new StringReader(kafkaProducerProperties));
			producer = new KafkaProducer<>(properties);
			System.out.println("[loading properties of kafka]: \n[" + properties + "]");
		} catch (Exception exception) {
			System.out.println("[failed init KafkaProducer]: \n" + exception.getMessage());
		}
		if (logToLocal) {
			log.info("[KafkaAppender]: properties={}", properties);
		}
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
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(getTopicName(), msg);
		System.out.println("[推送数据]:{}" + producerRecord);
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
					if (logToLocal) {
						log.info("[push to kafka]:{}", producerRecord);
						log.info("[push failed]:{}", exception.getMessage());
					}
					exception.printStackTrace();
				}
				System.out.println("[推送数据到kafka成功]:" + metadata.toString());
				if (logToLocal) {
					log.info("[推送数据到kafka成功]:{}", metadata);
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

	public boolean isLogToLocal() {
		return logToLocal;
	}

	public void setLogToLocal(boolean logToLocal) {
		this.logToLocal = logToLocal;
	}

	public String getKafkaProducerProperties() {
		return kafkaProducerProperties;
	}

	public void setKafkaProducerProperties(String kafkaProducerProperties) {
		this.kafkaProducerProperties = kafkaProducerProperties;
	}
}
