package com.demo.kafka.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
// @formatter:off
/**
 * @Service(	
 * 	loadbalance = "roundrobin", 
 * 	retries = 2, 
 * 	cluster = "failover", 
 * 	version = "1.0.0", 
 * 	application = "${dubbo.application.id}", 
 * 	protocol = "${dubbo.protocol.id}", 
 * 	registry = "${dubbo.registry.id}"
 * )
 *
 * @author fuhw/vencano
 * @date 2018-05-08
 */
// @formatter:on

@Component
public class LogsToKafkaServiceImplV1 implements LogsToKafkaService {

	private static final Logger log = LoggerFactory.getLogger("kafka-event");

	@Override
	public void produce(String msgContent) {
		if (StringUtils.isEmpty(msgContent)) {
			return;
		}
		log.info(msgContent);
	}


}