package com.ryuusen.kafka.client.serde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class GsonObjectDeser implements Deserializer<JsonElement>{

	JsonParser parser = new JsonParser();
	
	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public JsonElement deserialize(String topic, byte[] data) {
		if(null == data){
			return null;
		}
		return parser.parse(new String(data));
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
