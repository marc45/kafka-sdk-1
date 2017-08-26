package com.ryuusen.kafka.client.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public interface IProducer<K, V> extends Producer<K, V> {

	public void processConfigs(Properties props);

	public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record);

	public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record, Callback callback);

	public Future<RecordMetadata> sendAsync(K key, V value);

	public Future<RecordMetadata> sendAsync(K key, V value, Callback callback);

	public Future<RecordMetadata> sendAsync(V value);

	public Future<RecordMetadata> sendAsync(V value, Callback callback);

	public RecordMetadata sendSync(ProducerRecord<K, V> record) throws InterruptedException, ExecutionException;

	public RecordMetadata sendSync(ProducerRecord<K, V> record, Callback callback)
			throws InterruptedException, ExecutionException;

	public RecordMetadata sendSync(K key, V value, Callback callback) throws InterruptedException, ExecutionException;

	public RecordMetadata sendSync(K key, V value) throws InterruptedException, ExecutionException;

	public RecordMetadata sendSync(V value) throws InterruptedException, ExecutionException;

	public RecordMetadata sendSync(V value, Callback callback) throws InterruptedException, ExecutionException;
	
	public void setDefaultTopic(String defaultTopic);
	
	public String getDefaultTopic(String defaultTopic);
}
