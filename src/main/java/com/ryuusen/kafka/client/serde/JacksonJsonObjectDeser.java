package com.ryuusen.kafka.client.serde;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonJsonObjectDeser implements Deserializer<JsonNode>{
	
	public static final String JACKSON_OBJECT_MAPPER = "jackson.object.mapper";
	
	ObjectMapper mapper;
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Object configMapper = configs.get(JACKSON_OBJECT_MAPPER);
		if(null != configMapper && configMapper instanceof ObjectMapper){
			mapper = (ObjectMapper) configMapper;
		} else {
			mapper = new ObjectMapper();
		}
	}

	@Override
	public JsonNode deserialize(String topic, byte[] data) {
		if(null == data){
			return null;
		}
		try {
			return mapper.readTree(data);
		} catch (IOException e) {
			// TODO log
			throw new SerializationException(e);
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
