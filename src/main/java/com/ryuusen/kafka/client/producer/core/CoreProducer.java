package com.ryuusen.kafka.client.producer.core;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public class CoreProducer<K, V> implements Producer<K, V>{

	private Producer<K, V> primaryProducer;
	
	public CoreProducer(Properties props){
		primaryProducer = new KafkaProducer<K, V>(props);
	}
	
	@Deprecated
	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		return this.sendAsync(record);
	}
	
	@Deprecated
	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		return this.sendAsync(record, callback);
	}
		
	public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {
		return this.sendAsync(record, null);
	}

	public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record, Callback callback) {
		return primaryProducer.send(record, callback);
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
	
	@Override
	public void flush() {
		primaryProducer.flush();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return primaryProducer.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return primaryProducer.metrics();
	}

	@Override
	public void close() {
		primaryProducer.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		primaryProducer.close(timeout, unit);
	}

}
