package com.demo.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TempEvent {
	private String name, evenId;
}