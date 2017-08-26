package com.ryuusen.kafka.client.producer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.ryuusen.kafka.client.IClientConfigBuilder;

public class CoreProducer<K, V> implements IProducer<K, V>{
	
	Producer<K, V> baseProducer;
	String defaultTopic;
	
	public CoreProducer(Properties props){
		processConfigs(props);
		baseProducer = new KafkaProducer<>(props);
	}
	
	public CoreProducer(Properties props, Producer<K, V> baseProducer){
		processConfigs(props);
		this.baseProducer = baseProducer;
	}
	
	@Override
	public void processConfigs(Properties props){
		this.defaultTopic = props.getProperty(IClientConfigBuilder.DEFAULT_TOPIC_CONFIG);
	}

	@Override
    public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record) {
        return this.send(record);
    }

	@Override
    public Future<RecordMetadata> sendAsync(ProducerRecord<K, V> record, Callback callback) {
        return this.send(record, callback);
    }
	
	@Override
	public Future<RecordMetadata> sendAsync(K key, V value) {
		return this.sendAsync(key, value, null);
	}
	
	@Override
	public Future<RecordMetadata> sendAsync(K key, V value, Callback callback) {
		checkDefaultTopic();
		return this.sendAsync(new ProducerRecord<K, V>(defaultTopic, key, value), callback);
	}
	
	@Override
	public Future<RecordMetadata> sendAsync(V value) {
		return this.sendAsync(value, null);
	}
	
	@Override
	public Future<RecordMetadata> sendAsync(V value, Callback callback) {
		return this.sendAsync(null, value, callback);
	}
	
	@Override
	public RecordMetadata sendSync(ProducerRecord<K, V> record) throws InterruptedException, ExecutionException{
		return this.sendSync(record, null);
	}
	
	@Override
	public RecordMetadata sendSync(ProducerRecord<K, V> record, Callback callback) throws InterruptedException, ExecutionException{
		return this.sendAsync(record, callback).get();
	}

	@Override
	public RecordMetadata sendSync(K key, V value, Callback callback) throws InterruptedException, ExecutionException{
		checkDefaultTopic();
		return this.sendSync(new ProducerRecord<K, V>(defaultTopic, key, value), callback);
	}
	
	@Override
	public RecordMetadata sendSync(K key, V value) throws InterruptedException, ExecutionException{
		return this.sendSync(key, value, null);
	}
	
	@Override
	public RecordMetadata sendSync(V value) throws InterruptedException, ExecutionException{
		return this.sendSync(null, value, null);
	}
	
	@Override
	public RecordMetadata sendSync(V value, Callback callback) throws InterruptedException, ExecutionException{
		return this.sendSync(null, value, callback);
	}
	
	protected void checkDefaultTopic(){
		if(null == defaultTopic || defaultTopic.isEmpty()){
			throw new IllegalStateException("Attempted to send message to null/empty defaultTopic");
		}
	}

	@Override
	public void initTransactions() {
		baseProducer.initTransactions();	
	}

	@Override
	public void beginTransaction() throws ProducerFencedException {
		baseProducer.beginTransaction();
	}

	@Override
	public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
			throws ProducerFencedException {
		baseProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
	}

	@Override
	public void commitTransaction() throws ProducerFencedException {
		baseProducer.commitTransaction();
	}

	@Override
	public void abortTransaction() throws ProducerFencedException {
		baseProducer.abortTransaction();
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
		return send(record, null);
	}

	@Override
	public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
		return baseProducer.send(record, callback);
	}

	@Override
	public void flush() {
		baseProducer.flush();
	}

	@Override
	public List<PartitionInfo> partitionsFor(String topic) {
		return baseProducer.partitionsFor(topic);
	}

	@Override
	public Map<MetricName, ? extends Metric> metrics() {
		return baseProducer.metrics();
	}

	@Override
	public void close() {
		baseProducer.close();
	}

	@Override
	public void close(long timeout, TimeUnit unit) {
		baseProducer.close(timeout, unit);
	}

	@Override
	public void setDefaultTopic(String defaultTopic) {
		this.defaultTopic = defaultTopic;
	}

	@Override
	public String getDefaultTopic(String defaultTopic) {
		return defaultTopic;
	}

}
