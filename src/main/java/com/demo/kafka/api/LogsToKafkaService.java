package com.demo.kafka.api;

public interface LogsToKafkaService {

	void produce(String msgContent);
	
}
