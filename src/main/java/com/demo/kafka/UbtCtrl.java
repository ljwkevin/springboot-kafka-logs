package com.demo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("ubt")
public class UbtCtrl {

	private static final Logger log = LoggerFactory.getLogger("kafka-event");

	@GetMapping("/dowork")
	public void login() {

		for (int i = 0; i < 10000; i++) {
			log.info("user-dowork:xiaolang"+i);

		}
	}

	
}
