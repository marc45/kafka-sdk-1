package com.ryuusen.kafka.client.producer.core;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CoreProducer<K, V> extends AbstractProducer<K, V>{
	
	public CoreProducer(Properties props){
		this.setPrimary(new KafkaProducer<K, V>(props));
	}
	
	public Future<RecordMetadata> sendAsync(String topic, K key, V value) {
		return this.sendAsync(topic, key, value, null);
	}
	
	public Future<RecordMetadata> sendAsync(String topic, K key, V value, Callback callback) {
		return this.sendAsync(new ProducerRecord<K, V>(topic, key, value), callback);
	}
	
	public Future<RecordMetadata> sendAsync(String topic, V value) {
		return this.sendAsync(topic, value, null);
	}
	
	public Future<RecordMetadata> sendAsync(String topic, V value, Callback callback) {
		return this.sendAsync(topic, null, value, callback);
	}
	
	public RecordMetadata sendSync(ProducerRecord<K, V> record) throws InterruptedException, ExecutionException{
		return this.sendSync(record, null);
	}
	
	public RecordMetadata sendSync(ProducerRecord<K, V> record, Callback callback) throws InterruptedException, ExecutionException{
		return this.sendAsync(record, callback).get();
	}

	public RecordMetadata sendSync(String topic, K key, V value, Callback callback) throws InterruptedException, ExecutionException{
		return this.sendSync(new ProducerRecord<K, V>(topic, key, value), callback);
	}
	
	public RecordMetadata sendSync(String topic, K key, V value) throws InterruptedException, ExecutionException{
		return this.sendSync(topic, key, value, null);
	}
	
	public RecordMetadata sendSync(String topic, V value) throws InterruptedException, ExecutionException{
		return this.sendSync(topic, null, value, null);
	}
	
	public RecordMetadata sendSync(String topic, V value, Callback callback) throws InterruptedException, ExecutionException{
		return this.sendSync(topic, null, value, callback);
	}
}
