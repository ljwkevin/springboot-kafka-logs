package com.demo.kafka.api;

import java.util.List;

public interface LogsToKafkaService {

	void produce(String msgContent);
	
	void produceBatch(List<String> msgContents);
	
	void produceBatchByDirect(List<String> msgContents);
	
}
