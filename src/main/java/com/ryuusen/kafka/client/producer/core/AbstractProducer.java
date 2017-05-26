package com.ryuusen.kafka.client.producer.core;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;

public abstract class AbstractProducer<K, V> implements Producer<K, V>{

	private Producer<K, V> primary;
	
	public AbstractProducer(){
		
	}
	
	public AbstractProducer(Map<String, Object> props){
		primary = new KafkaProducer<K, V>(props);
	}
	
	public AbstractProducer(Properties props){
		primary = new KafkaProducer<K, V>(props);
	}
	
	protected Producer<K, V> primary(){
		return primary;
	}
	
	protected void setPrimary(Producer<K, V> primary){
		this.primary = primary;
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
		return primary.send(record, callback);
	}
	
	@Override
	public void flush() {
		primary.flush();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return primary.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return primary.metrics();
	}

	@Override
	public void close() {
		primary.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		primary.close(timeout, unit);
	}

}
