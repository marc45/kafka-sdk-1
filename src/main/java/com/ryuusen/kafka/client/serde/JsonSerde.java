package com.ryuusen.kafka.client.serde;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerde<T> implements Deserializer<T>, Serializer<T> {

	public static final String JACKSON_OBJECT_MAPPER = "jackson.object.mapper";
	public static final String JACKSON_KEY_DESERIALIZER_CLASS = "jackson.key.deserializer.class";
	public static final String JACKSON_VALUE_DESERIALIZER_CLASS = "jackson.value.deserializer.class";

	ObjectMapper mapper;
	Class deserClass;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		Object configMapper = configs.get(JACKSON_OBJECT_MAPPER);
		if(null != configMapper && configMapper instanceof ObjectMapper){
			mapper = (ObjectMapper) configMapper;
		} else {
			mapper = new ObjectMapper();
		}
		Object dsClass = configs.get(isKey ? JACKSON_KEY_DESERIALIZER_CLASS : JACKSON_VALUE_DESERIALIZER_CLASS);
		if (null != dsClass) {
			if (dsClass instanceof String) {
				try {
					dsClass = Class.forName((String) dsClass);
				} catch (ClassNotFoundException e) {
					throw new SerializationException("Unable to find class: " + (String) dsClass);
				}
			}
			if (dsClass instanceof Class) {
				deserClass = (Class) dsClass;
			}
		}
	}
	
	@Override
	public byte[] serialize(String topic, T data) {
		if (null == data) {
			return null;
		}
		try {
			return mapper.writeValueAsBytes(data);
		} catch (JsonProcessingException e) {
			//log later
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		if(null == data){
			return null;
		}
		if(null == deserClass){
			throw new IllegalStateException("Attempted to deserialize with no Deserialization Class");
		}
		try {
			return (T) mapper.readValue(data, deserClass);
		} catch (IOException e) {
			// TODO log
			throw new SerializationException(e);
		}
	}

	@Override
	public void close() {
		// NOOP
	}

}
