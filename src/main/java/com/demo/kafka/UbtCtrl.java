package com.demo.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.demo.kafka.api.LogsToKafkaService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("ubt")
public class UbtCtrl {

	private static final ObjectMapper mapper = new ObjectMapper();

	@Autowired
	private LogsToKafkaService LogsToKafkaService;

	@GetMapping("/dowork")
	public void login() {
		List<String> mStrings = new ArrayList<>(100000);
		for (int i = 0; i < 100000; i++) {
			try {
				mStrings.add(mapper.writeValueAsString(new TempEvent(UUID.randomUUID().toString(), i + "")));
			} catch (JsonProcessingException e) {
			}
		}
		LogsToKafkaService.produceBatchByDirect(mStrings);
	}
}
