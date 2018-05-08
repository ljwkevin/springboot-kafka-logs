package com.demo.kafka;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("ubt")
public class UbtCtrl {

	private static final Logger log = LoggerFactory.getLogger("kafka-event");

	private static final ObjectMapper mapper = new ObjectMapper();

	@GetMapping("/dowork")
	public void login() {
		for (int i = 0; i < 100000; i++) {
			try {
				log.info(mapper.writeValueAsString(new TempEvent(UUID.randomUUID().toString(), i + "")));
			} catch (JsonProcessingException e) {
			}
		}
	}

}
